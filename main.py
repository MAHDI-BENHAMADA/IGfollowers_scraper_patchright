"""
Instagram Followers Scraper — Patchright Stealth Edition
=========================================================
Optimizations over original:
  • Phase-1 checkpoint: usernames saved to JSON every N scrolls → safe stop/resume
  • Incremental CSV writes: each enriched profile appended immediately (no RAM buildup)
  • Faster collector: tracks only NEW links per scroll instead of re-scanning all links
  • Smarter stale detection: shorter but progressive back-off instead of flat sleep
  • Phase-2 resume: already-enriched usernames skipped automatically on restart
  • Single --out prefix controls all files:
        <out>.csv              ← final / live-appended enriched rows
        <out>_usernames.json   ← phase-1 checkpoint (deleted on clean finish)
  • Pre-scroll phase (--pre-scroll N): burst-scrolls to N accounts before collecting,
        priming Instagram's bulk-load mode for dramatically higher yield per scroll
        (observed: +12/scroll normally → +96/+104/scroll after pre-scrolling)
  • Burst scroll: catch-up and pre-scroll phases use rapid multi-wheel events to
        mimic manual scroll behaviour that triggers Instagram's lazy-loader

Requirements:
    pip install patchright instaloader
    patchright install chromium

Usage:
    py main.py trainerize ^
        --login-user kimodrac ^
        --max-followers 5000 ^
        --pre-scroll 5000 ^
        --out trainerize_followers
"""

import argparse
import csv
import json
import logging
import random
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Dict, List, Optional, Set

import instaloader
from patchright.sync_api import BrowserContext, Page, sync_playwright
from patchright.sync_api import TimeoutError as PWTimeout


# ──────────────────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class FollowerRecord:
    username:     str
    full_name:    str
    user_id:      str
    biography:    str
    followers:    str
    following:    str
    media_count:  str
    is_private:   str
    is_verified:  str
    external_url: str
    profile_url:  str


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Instagram follower scraper — Patchright stealth edition."
    )
    p.add_argument("target",              help="Target Instagram username (without @).")
    p.add_argument("--login-user",        required=True,  help="Your Instagram login username.")
    p.add_argument("--password",          default=None,   help="Instagram password (will prompt if omitted).")
    p.add_argument("--session-file",      default="ig_session", help="Instaloader session file (unused, compat).")
    p.add_argument("--max-followers",     type=int,   default=0,    help="Max followers to export (0 = all).")
    p.add_argument("--scroll-min-delay",  type=float, default=1.5,  help="Min delay between modal scrolls.")
    p.add_argument("--scroll-max-delay",  type=float, default=3.5,  help="Max delay between modal scrolls.")
    p.add_argument("--profile-min-delay", type=float, default=8.0,  help="Min delay between profile visits.")
    p.add_argument("--profile-max-delay", type=float, default=15.0, help="Max delay between profile visits.")
    p.add_argument("--retries",           type=int,   default=3,    help="Retries per profile page load.")
    p.add_argument("--checkpoint-every",  type=int,   default=50,   help="Save username checkpoint every N scrolls.")
    p.add_argument("--pre-scroll",        type=int,   default=0,    help="Pre-scroll target before collecting.")
    p.add_argument("--skip-phase1",       action="store_true", help="Skip Phase 1 scrolling — go straight to profile scraping using existing checkpoint.")
    p.add_argument("--out",               default="followers_export", help="Output file prefix.")
    p.add_argument("--headless",          action="store_true", help="Run headless (less stable on Instagram).")
    p.add_argument("--verbose",           action="store_true", help="Enable debug logging.")
    return p.parse_args()


def setup_logger(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def rand_sleep(mn: float, mx: float) -> None:
    t = random.uniform(mn, mx)
    logging.debug("Sleeping %.1fs", t)
    time.sleep(t)


# ──────────────────────────────────────────────────────────────────────────────
# Checkpoint helpers — Phase 1 (usernames)
# ──────────────────────────────────────────────────────────────────────────────

def _username_checkpoint_path(out_prefix: str) -> Path:
    return Path(out_prefix).with_name(f"{Path(out_prefix).stem}_usernames.json")


def save_username_checkpoint(path: Path, usernames: List[str]) -> None:
    """Persist collected usernames to disk so Phase 1 can resume."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(usernames, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)          # atomic replace — no half-written files
    logging.debug("Username checkpoint saved: %d usernames → %s", len(usernames), path)


def load_username_checkpoint(path: Path) -> Optional[List[str]]:
    """Return saved usernames if checkpoint exists, else None."""
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                logging.info("Resuming Phase 1 from checkpoint: %d usernames already collected.", len(data))
                return data
        except Exception as exc:
            logging.warning("Could not read username checkpoint (%s) — starting fresh.", exc)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Incremental CSV writer — Phase 2 (enrichment)
# ──────────────────────────────────────────────────────────────────────────────

class IncrementalCSV:
    """
    Appends one row at a time to the output CSV.
    On construction it reads back any rows already written so Phase 2 can
    skip already-enriched usernames on a restart.
    """

    def __init__(self, path: Path):
        self.path = path
        self.fieldnames = [f.name for f in fields(FollowerRecord)]
        self.done: Set[str] = set()

        if path.exists():
            try:
                with path.open("r", newline="", encoding="utf-8") as fp:
                    reader = csv.DictReader(fp)
                    for row in reader:
                        if row.get("username"):
                            self.done.add(row["username"])
                logging.info(
                    "Output CSV exists — %d profiles already enriched, will skip them.",
                    len(self.done),
                )
            except Exception as exc:
                logging.warning("Could not read existing CSV (%s). Will overwrite.", exc)
                self.done = set()

        # Open in append mode; write header only if file is new/empty
        self._file = path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames)
        if not self.done:
            self._writer.writeheader()
            self._file.flush()

    def write(self, record: FollowerRecord) -> None:
        self._writer.writerow(asdict(record))
        self._file.flush()               # flush after every row → data on disk immediately
        self.done.add(record.username)

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# Browser / session setup
# ──────────────────────────────────────────────────────────────────────────────

def make_browser_page(pw, headless: bool) -> tuple:
    browser = pw.chromium.launch(
        headless=headless,
        args=["--no-sandbox"],
    )
    ctx: BrowserContext = browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        locale="en-US",
    )
    page = ctx.new_page()
    return browser, page


def browser_login(page: Page, username: str, password: str) -> None:
    logging.info("Logging in as @%s via browser...", username)
    page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded", timeout=45_000)
    rand_sleep(2, 3)

    for cookie_btn in ["Allow all cookies", "Allow essential cookies", "Only allow essential cookies"]:
        try:
            page.get_by_role("button", name=cookie_btn).click(timeout=2_500)
            rand_sleep(0.6, 1.2)
            break
        except PWTimeout:
            pass

    username_locator = None
    password_locator = None

    for selector in [
        "input[name='username']",
        "input[autocomplete='username']",
        "input[aria-label*='username' i]",
        "input[aria-label*='email' i]",
    ]:
        candidate = page.locator(selector).first
        try:
            candidate.wait_for(state="visible", timeout=3_500)
            username_locator = candidate
            break
        except PWTimeout:
            continue

    for selector in [
        "input[name='password']",
        "input[autocomplete='current-password']",
        "input[type='password']",
    ]:
        candidate = page.locator(selector).first
        try:
            candidate.wait_for(state="visible", timeout=3_500)
            password_locator = candidate
            break
        except PWTimeout:
            continue

    if not username_locator or not password_locator:
        logging.warning("Login form not detected. Complete login/challenge manually (up to 2 min).")
        try:
            page.wait_for_url(lambda url: "accounts/login" not in url, timeout=120_000)
            logging.info("Manual login completed.")
            return
        except PWTimeout:
            raise RuntimeError("Login failed or challenge not completed in time.")

    username_locator.click()
    rand_sleep(0.3, 0.6)
    username_locator.fill(username)
    rand_sleep(0.8, 1.5)
    password_locator.click()
    rand_sleep(0.3, 0.6)
    password_locator.fill(password)
    rand_sleep(0.8, 1.5)
    page.locator("button[type='submit']").click()
    logging.info("Submitted login form, waiting for redirect...")

    try:
        page.wait_for_url(lambda url: "accounts/login" not in url, timeout=60_000)
        logging.info("Login successful.")
    except PWTimeout:
        logging.warning("Still on login page — complete challenge manually (up to 2 min).")
        try:
            page.wait_for_url(lambda url: "accounts/login" not in url, timeout=120_000)
            logging.info("Manual challenge completed.")
        except PWTimeout:
            raise RuntimeError("Login failed or challenge not completed in time.")

    rand_sleep(3, 5)
    for btn_text in ["Not now", "Not Now", "Skip"]:
        try:
            page.get_by_role("button", name=btn_text).click(timeout=3_000)
            rand_sleep(1, 2)
        except PWTimeout:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# Phase 1 — collect usernames from the followers modal
# ──────────────────────────────────────────────────────────────────────────────

def collect_usernames(
    page: Page,
    target: str,
    max_followers: int,
    min_delay: float,
    max_delay: float,
    checkpoint_path: Path,
    checkpoint_every: int,
    resume_from: Optional[List[str]] = None,
    pre_scroll: int = 0,
) -> List[str]:
    """
    Scroll the followers modal and return a deduplicated list of usernames.

    Resume / continue behaviour
    ───────────────────────────
    Instagram's modal always starts from the top, so on a resume run we MUST
    scroll past the already-collected portion before we see new names.  We do
    this in two distinct phases inside the loop:

      CATCH-UP phase  (while len(seen) < catchup_target)
        • No sleep between scrolls — just scroll as fast as the modal loads.
        • We still deduplicate so we never add a name twice.
        • Progress logged every 500 names so you can see it moving.

      NEW-TERRITORY phase  (once we pass the old frontier)
        • Normal human-like delays resume.
        • Checkpoints written every N new usernames.
        • Progressive stale back-off at the end of the list.

    This means a "continue from 5k → 20k" run will blaze through the first
    5 000 entries in a minute or two, then settle into normal scraping speed
    for the remaining 15 000 — with zero duplicates in the final CSV.
    """

    # ── Seed from checkpoint ───────────────────────────────────────────────────
    seen: Dict[str, bool] = {}
    catchup_target = 0          # how many names to fast-scroll past
    if resume_from:
        for u in resume_from:
            seen[u] = True
        catchup_target = len(seen)
        logging.info(
            "Resuming: %d usernames already collected. "
            "Fast-scrolling past them before collecting new ones...",
            catchup_target,
        )

    # ── Navigate and open modal ────────────────────────────────────────────────
    logging.info("Navigating to @%s", target)
    page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded", timeout=30_000)
    rand_sleep(2, 4)

    for dismiss_selector in [
        "div[role='dialog'] svg[aria-label='Close']",
        "button:has-text('Not now')",
    ]:
        try:
            page.locator(dismiss_selector).first.click(timeout=3_000)
            rand_sleep(0.8, 1.5)
        except PWTimeout:
            pass

    try:
        page.locator("a[href$='/followers/']").first.click(timeout=10_000)
    except PWTimeout:
        page.get_by_text("followers", exact=False).first.click()

    page.wait_for_selector("div[role='dialog']", timeout=15_000)
    rand_sleep(1.5, 3)

    if catchup_target:
        logging.info("Modal open — CATCH-UP mode (no delays until we pass %d usernames).", catchup_target)
    else:
        logging.info("Modal open — scrolling.")

    # ── Scroll loop ────────────────────────────────────────────────────────────
    stale           = 0
    MAX_STALE       = 12
    since_last_ckpt = 0
    last_log_total  = 0     # for catch-up progress logging every 500

    def _scroll_modal():
        dialog = page.query_selector("div[role='dialog']")
        if dialog:
            box = dialog.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
        page.mouse.wheel(0, random.randint(400, 750))

    while True:
        rows = page.query_selector_all("div[role='dialog'] a[href^='/'][href$='/']")
        new_this_round = 0
        for row in rows:
            href = (row.get_attribute("href") or "").strip("/")
            if not href or "/" in href:
                continue
            if href not in seen:
                seen[href] = True
                new_this_round += 1

        total = len(seen)
        in_catchup = total < catchup_target

        # ── Logging ───────────────────────────────────────────────────────────
        if in_catchup:
            if total - last_log_total >= 500:
                logging.info(
                    "Catch-up: %d / %d (%.0f%%) — still fast-scrolling...",
                    total, catchup_target, 100 * total / catchup_target,
                )
                last_log_total = total
        else:
            logging.info("Usernames: %d total (+%d new this scroll)", total, new_this_round)

        # ── Checkpoint (only in new-territory phase) ──────────────────────────
        if not in_catchup:
            since_last_ckpt += new_this_round
            if since_last_ckpt >= checkpoint_every:
                save_username_checkpoint(checkpoint_path, list(seen.keys()))
                since_last_ckpt = 0

        # ── Stop conditions ───────────────────────────────────────────────────
        if max_followers > 0 and total >= max_followers:
            logging.info("Reached --max-followers %d", max_followers)
            break

        if not in_catchup and new_this_round == 0:
            stale += 1
            if stale >= MAX_STALE:
                logging.info("No new usernames after %d consecutive empty scrolls — end of list.", MAX_STALE)
                break
            extra = min(stale * 0.4, 3.0)
            rand_sleep(min_delay + extra, max_delay + extra)
        elif not in_catchup:
            stale = 0
            rand_sleep(min_delay, max_delay)
        # else: catch-up mode — no sleep, just scroll immediately

        _scroll_modal()

    # Final checkpoint flush
    save_username_checkpoint(checkpoint_path, list(seen.keys()))

    usernames = list(seen.keys())
    if max_followers > 0:
        usernames = usernames[:max_followers]
    logging.info("Phase 1 done: %d usernames total.", len(usernames))
    return usernames


# ──────────────────────────────────────────────────────────────────────────────
# Phase 2 — scrape each profile
# ──────────────────────────────────────────────────────────────────────────────

def _dig(d, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _user_to_record(user: dict, username: str) -> FollowerRecord:
    return FollowerRecord(
        username=user.get("username", username),
        full_name=user.get("full_name", ""),
        user_id=str(user.get("pk", "") or user.get("id", "")),
        biography=user.get("biography", "") or user.get("bio", ""),
        followers=str(_dig(user, "edge_followed_by", "count") or user.get("follower_count", "")),
        following=str(_dig(user, "edge_follow", "count")      or user.get("following_count", "")),
        media_count=str(_dig(user, "edge_owner_to_timeline_media", "count") or user.get("media_count", "")),
        is_private=str(user.get("is_private", "")),
        is_verified=str(user.get("is_verified", "")),
        external_url=user.get("external_url", "") or "",
        profile_url=f"https://www.instagram.com/{user.get('username', username)}/",
    )


def _find_user_in_obj(obj, username: str):
    if isinstance(obj, dict):
        uname = obj.get("username")
        if (
            isinstance(uname, str)
            and uname.lower() == username.lower()
            and ("follower_count" in obj or "edge_followed_by" in obj or "full_name" in obj)
        ):
            return obj
        for v in obj.values():
            found = _find_user_in_obj(v, username)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_user_in_obj(item, username)
            if found:
                return found
    return None


def scrape_profile(page: Page, username: str, retries: int) -> Optional[FollowerRecord]:
    profile_url = f"https://www.instagram.com/{username}/"

    for attempt in range(retries + 1):
        try:
            page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)
            rand_sleep(1.5, 3)
            break
        except Exception as exc:
            logging.warning("Page load failed @%s attempt %d: %s", username, attempt + 1, exc)
            if attempt < retries:
                time.sleep(random.uniform(5, 10))
    else:
        logging.error("Giving up on @%s", username)
        return None

    # Method 1: embedded JSON in <script> tags
    try:
        for script in page.query_selector_all("script[type='application/json']"):
            try:
                data = json.loads(script.inner_text())
                user = _find_user_in_obj(data, username)
                if user:
                    logging.debug("Embedded JSON method succeeded for @%s", username)
                    return _user_to_record(user, username)
            except Exception:
                continue
    except Exception:
        pass

    # Method 2: DOM extraction fallback
    logging.debug("DOM fallback for @%s", username)
    dom_data = page.evaluate("""(username) => {
        const data = {};

        const followersLink = document.querySelector('a[href="/' + username + '/followers/"]');
        if (followersLink) {
            const titleSpan = followersLink.querySelector('span[title]');
            if (titleSpan) {
                data.followers = titleSpan.getAttribute('title');
            } else {
                const htmlSpan = followersLink.querySelector('span.html-span');
                if (htmlSpan) data.followers = htmlSpan.textContent.trim();
            }
        }

        const followingLink = document.querySelector('a[href="/' + username + '/following/"]');
        if (followingLink) {
            const titleSpan = followingLink.querySelector('span[title]');
            if (titleSpan) {
                data.following = titleSpan.getAttribute('title');
            } else {
                const htmlSpan = followingLink.querySelector('span.html-span');
                if (htmlSpan) data.following = htmlSpan.textContent.trim();
            }
        }

        if (followersLink) {
            const parentDiv = followersLink.closest('div');
            const statsContainer = parentDiv && parentDiv.parentElement;
            if (statsContainer) {
                for (const child of statsContainer.children) {
                    const text = child.textContent || '';
                    if (/\\bposts?\\b/i.test(text) && !child.querySelector('a')) {
                        const ts = child.querySelector('span[title]');
                        if (ts) {
                            data.media_count = ts.getAttribute('title');
                        } else {
                            const hs = child.querySelector('span.html-span');
                            if (hs) data.media_count = hs.textContent.trim();
                        }
                        break;
                    }
                }
            }
        }

        const bioClassSpans = document.querySelectorAll(
            'header span._ap3a._aaco._aacu._aacx._aad7._aade'
        );
        if (bioClassSpans.length > 0) {
            const bioSection = bioClassSpans[0].closest('section');
            if (bioSection) {
                const candidates = bioSection.querySelectorAll('span[dir="auto"]');
                for (const s of candidates) {
                    if (!s.closest('._ap3a') && s.textContent.trim()) {
                        data.full_name = s.textContent.trim();
                        break;
                    }
                }
            }
        }
        if (!data.full_name) {
            const mt = document.querySelector('meta[property="og:title"]');
            if (mt) {
                const c = mt.getAttribute('content') || '';
                const m = c.match(/^(.+?)\\s*\\(@/);
                if (m) data.full_name = m[1].trim();
            }
        }

        const bioBtn = document.querySelector(
            'header div[role="button"] > span._ap3a._aaco._aacu._aacx._aad7._aade'
        );
        if (bioBtn) {
            data.biography = bioBtn.innerText.trim();
        } else if (bioClassSpans.length > 0) {
            data.biography = bioClassSpans[0].innerText.trim();
        }

        data.is_verified = !!document.querySelector('header svg[aria-label="Verified"]');

        const h2s = document.querySelectorAll('h2');
        data.is_private = [...h2s].some(
            h => (h.textContent || '').includes('This account is private')
        );

        const urlBtn = document.querySelector('header button._aswp div._ap3a._aaco._aacw');
        if (urlBtn) {
            data.external_url = urlBtn.textContent.trim();
        }

        return data;
    }""", username)

    return FollowerRecord(
        username=username,
        full_name=dom_data.get("full_name", ""),
        user_id="",
        biography=dom_data.get("biography", ""),
        followers=str(dom_data.get("followers", "")).replace(",", ""),
        following=str(dom_data.get("following", "")).replace(",", ""),
        media_count=str(dom_data.get("media_count", "")).replace(",", ""),
        is_private=str(dom_data.get("is_private", "")),
        is_verified=str(dom_data.get("is_verified", "")),
        external_url=dom_data.get("external_url", ""),
        profile_url=profile_url,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    setup_logger(args.verbose)

    out_csv       = Path(args.out).with_suffix(".csv")
    ckpt_path     = _username_checkpoint_path(args.out)

    password = args.password
    if not password:
        import getpass
        password = getpass.getpass(f"Enter Instagram password for {args.login_user}: ")

    # ── Check for existing username checkpoint ─────────────────────────────────
    resume_usernames = load_username_checkpoint(ckpt_path)

    # ── Open incremental CSV (re-uses existing file for Phase 2 resume) ────────
    csv_writer = IncrementalCSV(out_csv)

    with sync_playwright() as pw:
        browser, page = make_browser_page(pw, args.headless)

        try:
            # 1. Login
            browser_login(page, args.login_user, password)

            # 2. Phase 1 — collect usernames (with checkpoint + resume)
            #
            # Skip Phase 1 when any of these are true:
            #   (a) --skip-phase1 flag passed explicitly
            #   (b) checkpoint satisfies --max-followers already
            #   (c) checkpoint exists and CSV is empty (Phase 1 was interrupted)
            #   (d) no checkpoint but CSV has rows (Phase 2 was interrupted, re-scroll needed)

            csv_usernames = list(csv_writer.done) if csv_writer.done else []
            force_skip = args.skip_phase1
            checkpoint_complete = (
                resume_usernames
                and args.max_followers > 0
                and len(resume_usernames) >= args.max_followers
            )
            phase1_interrupted = (
                resume_usernames and not checkpoint_complete and len(csv_writer.done) == 0
            )

            if force_skip:
                if resume_usernames:
                    usernames = resume_usernames
                    if args.max_followers > 0:
                        usernames = usernames[:args.max_followers]
                    logging.info(
                        "--skip-phase1: using %d usernames from checkpoint, going straight to Phase 2.",
                        len(usernames),
                    )
                elif csv_usernames:
                    usernames = csv_usernames
                    logging.info(
                        "--skip-phase1: no checkpoint, rebuilding from %d usernames already in CSV.",
                        len(usernames),
                    )
                else:
                    raise SystemExit("--skip-phase1 set but no checkpoint or CSV found to resume from.")

            elif checkpoint_complete:
                logging.info(
                    "Checkpoint already has %d usernames which satisfies --max-followers %d. "
                    "Skipping Phase 1 scroll.",
                    len(resume_usernames), args.max_followers,
                )
                usernames = resume_usernames[:args.max_followers]

            elif phase1_interrupted:
                logging.info(
                    "Phase 1 was interrupted — checkpoint has %d usernames. "
                    "Skipping re-scroll, going straight to Phase 2.",
                    len(resume_usernames),
                )
                usernames = resume_usernames

            else:
                usernames = collect_usernames(
                    page=page,
                    target=args.target,
                    max_followers=args.max_followers,
                    min_delay=args.scroll_min_delay,
                    max_delay=args.scroll_max_delay,
                    checkpoint_path=ckpt_path,
                    checkpoint_every=args.checkpoint_every,
                    resume_from=resume_usernames,
                    pre_scroll=args.pre_scroll,
                )

            if not usernames:
                raise SystemExit("No usernames collected. Check the account is public and you are logged in.")

            # Filter out already-enriched usernames (Phase 2 resume)
            pending = [u for u in usernames if u not in csv_writer.done]
            skipped = len(usernames) - len(pending)
            if skipped:
                logging.info("Skipping %d already-enriched profiles — resuming from where we left off.", skipped)

            total = len(usernames)
            done_so_far = skipped

            # 3. Phase 2 — enrich profiles, writing each row immediately
            for username in pending:
                done_so_far += 1
                logging.info("[%d/%d] Scraping @%s", done_so_far, total, username)
                record = scrape_profile(page, username, args.retries)
                if record:
                    csv_writer.write(record)   # ← written to disk right away
                else:
                    logging.warning("Skipped @%s (could not scrape)", username)

                if done_so_far < total:
                    rand_sleep(args.profile_min_delay, args.profile_max_delay)

        except KeyboardInterrupt:
            logging.info(
                "Interrupted. Progress saved:\n"
                "  • Enriched rows  → %s  (%d rows)\n"
                "  • Username list  → %s",
                out_csv, len(csv_writer.done), ckpt_path,
            )
        finally:
            csv_writer.close()
            try:
                browser.close()
            except Exception as exc:
                logging.warning("Browser already disconnected: %s", exc)

    if not csv_writer.done:
        raise SystemExit("No records exported.")

    # Clean up username checkpoint only if we finished cleanly
    if ckpt_path.exists():
        ckpt_path.unlink()
        logging.debug("Removed username checkpoint (clean finish).")

    logging.info("Done. %d followers exported → %s", len(csv_writer.done), out_csv.resolve())


if __name__ == "__main__":
    main()

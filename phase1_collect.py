import logging
import random
from pathlib import Path
from typing import Dict, List, Optional

from patchright.sync_api import Page
from patchright.sync_api import TimeoutError as PWTimeout

from utils import rand_sleep
from checkpoint import save_username_checkpoint

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
    """Scroll the followers modal and return a deduplicated list of usernames."""

    seen: Dict[str, bool] = {}
    catchup_target = 0
    if resume_from:
        for u in resume_from:
            seen[u] = True
        catchup_target = len(seen)
        logging.info(
            "Resuming: %d usernames already collected. "
            "Fast-scrolling past them before collecting new ones...",
            catchup_target,
        )

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
        logging.info("Modal open \u2014 CATCH-UP mode (no delays until we pass %d usernames).", catchup_target)
    else:
        logging.info("Modal open \u2014 scrolling.")

    stale           = 0
    MAX_STALE       = 12
    since_last_ckpt = 0
    last_log_total  = 0     

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

        if in_catchup:
            if total - last_log_total >= 500:
                logging.info(
                    "Catch-up: %d / %d (%.0f%%) \u2014 still fast-scrolling...",
                    total, catchup_target, 100 * total / catchup_target,
                )
                last_log_total = total
        else:
            logging.info("Usernames: %d total (+%d new this scroll)", total, new_this_round)

        if not in_catchup:
            since_last_ckpt += new_this_round
            if since_last_ckpt >= checkpoint_every:
                save_username_checkpoint(checkpoint_path, list(seen.keys()))
                since_last_ckpt = 0

        if max_followers > 0 and total >= max_followers:
            logging.info("Reached --max-followers %d", max_followers)
            break

        if not in_catchup and new_this_round == 0:
            stale += 1
            if stale >= MAX_STALE:
                logging.info("No new usernames after %d consecutive empty scrolls \u2014 end of list.", MAX_STALE)
                break
            extra = min(stale * 0.4, 3.0)
            rand_sleep(min_delay + extra, max_delay + extra)
        elif not in_catchup:
            stale = 0
            rand_sleep(min_delay, max_delay)

        _scroll_modal()

    save_username_checkpoint(checkpoint_path, list(seen.keys()))

    usernames = list(seen.keys())
    if max_followers > 0:
        usernames = usernames[:max_followers]
    logging.info("Phase 1 done: %d usernames total.", len(usernames))
    return usernames

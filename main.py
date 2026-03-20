"""
Instagram Followers Scraper — Patchright Stealth Edition
=========================================================

Usage:
    py main.py target_username ^
        --login-user your_username ^
        --max-followers 5000 ^
        --pre-scroll 5000 ^
        --out followers_export
"""

import argparse
import logging
from pathlib import Path

from patchright.sync_api import sync_playwright

from utils import setup_logger, rand_sleep
from checkpoint import get_username_checkpoint_path, load_username_checkpoint
from csv_writer import IncrementalCSV
from browser_auth import make_browser_page, browser_login
from phase1_collect import collect_usernames
from phase2_enrich import scrape_profile

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


def main() -> None:
    args = parse_args()
    setup_logger(args.verbose)

    out_csv       = Path(args.out).with_suffix(".csv")
    ckpt_path     = get_username_checkpoint_path(args.out)

    password = args.password
    if not password:
        import getpass
        password = getpass.getpass(f"Enter Instagram password for {args.login_user}: ")

    resume_usernames = load_username_checkpoint(ckpt_path)
    csv_writer = IncrementalCSV(out_csv)

    with sync_playwright() as pw:
        browser, page = make_browser_page(pw, args.headless)

        try:
            # 1. Login
            browser_login(page, args.login_user, password)

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

            pending = [u for u in usernames if u not in csv_writer.done]
            skipped = len(usernames) - len(pending)
            if skipped:
                logging.info("Skipping %d already-enriched profiles — resuming from where we left off.", skipped)

            total = len(usernames)
            done_so_far = skipped

            for username in pending:
                done_so_far += 1
                logging.info("[%d/%d] Scraping @%s", done_so_far, total, username)
                record = scrape_profile(page, username, args.retries)
                if record:
                    csv_writer.write(record) 
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

    if ckpt_path.exists():
        ckpt_path.unlink()
        logging.debug("Removed username checkpoint (clean finish).")

    logging.info("Done. %d followers exported → %s", len(csv_writer.done), out_csv.resolve())


if __name__ == "__main__":
    main()

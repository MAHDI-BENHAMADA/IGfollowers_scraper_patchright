import json
import logging
import random
import time
from typing import Optional

from patchright.sync_api import Page

from models import FollowerRecord
from utils import dig, find_user_in_obj, rand_sleep

def _user_to_record(user: dict, username: str) -> FollowerRecord:
    return FollowerRecord(
        username=user.get("username", username),
        full_name=user.get("full_name", ""),
        user_id=str(user.get("pk", "") or user.get("id", "")),
        biography=user.get("biography", "") or user.get("bio", ""),
        followers=str(dig(user, "edge_followed_by", "count") or user.get("follower_count", "")),
        following=str(dig(user, "edge_follow", "count")      or user.get("following_count", "")),
        media_count=str(dig(user, "edge_owner_to_timeline_media", "count") or user.get("media_count", "")),
        is_private=str(user.get("is_private", "")),
        is_verified=str(user.get("is_verified", "")),
        external_url=user.get("external_url", "") or "",
        profile_url=f"https://www.instagram.com/{user.get('username', username)}/",
    )

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
                user = find_user_in_obj(data, username)
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

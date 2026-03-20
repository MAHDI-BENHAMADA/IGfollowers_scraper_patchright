import logging
from patchright.sync_api import BrowserContext, Page
from patchright.sync_api import TimeoutError as PWTimeout
from utils import rand_sleep

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
        logging.warning("Still on login page \u2014 complete challenge manually (up to 2 min).")
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

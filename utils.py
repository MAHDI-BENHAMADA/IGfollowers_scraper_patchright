import logging
import random
import time

def setup_logger(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

def rand_sleep(mn: float, mx: float) -> None:
    t = random.uniform(mn, mx)
    logging.debug("Sleeping %.1fs", t)
    time.sleep(t)

def dig(d, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

def find_user_in_obj(obj, username: str):
    if isinstance(obj, dict):
        uname = obj.get("username")
        if (
            isinstance(uname, str)
            and uname.lower() == username.lower()
            and ("follower_count" in obj or "edge_followed_by" in obj or "full_name" in obj)
        ):
            return obj
        for v in obj.values():
            found = find_user_in_obj(v, username)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_user_in_obj(item, username)
            if found:
                return found
    return None

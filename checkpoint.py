import json
import logging
from pathlib import Path
from typing import List, Optional

def get_username_checkpoint_path(out_prefix: str) -> Path:
    return Path(out_prefix).with_name(f"{Path(out_prefix).stem}_usernames.json")

def save_username_checkpoint(path: Path, usernames: List[str]) -> None:
    """Persist collected usernames to disk so Phase 1 can resume."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(usernames, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)          # atomic replace \u2014 no half-written files
    logging.debug("Username checkpoint saved: %d usernames \u2192 %s", len(usernames), path)

def load_username_checkpoint(path: Path) -> Optional[List[str]]:
    """Return saved usernames if checkpoint exists, else None."""
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                logging.info("Resuming Phase 1 from checkpoint: %d usernames already collected.", len(data))
                return data
        except Exception as exc:
            logging.warning("Could not read username checkpoint (%s) \u2014 starting fresh.", exc)
    return None

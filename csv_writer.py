import csv
import logging
from dataclasses import asdict, fields
from pathlib import Path
from typing import Set

from models import FollowerRecord

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
                    "Output CSV exists \u2014 %d profiles already enriched, will skip them.",
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
        self._file.flush()               # flush after every row \u2192 data on disk immediately
        self.done.add(record.username)

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass

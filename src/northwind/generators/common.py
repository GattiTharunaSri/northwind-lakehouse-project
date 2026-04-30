"""Shared utilities for all data generators."""
from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from faker import Faker

# Seed for reproducibility — same day-offset always produces same data
def get_faker(seed: int = 42) -> Faker:
    """Get a seeded Faker instance for reproducible data."""
    fake = Faker(["en_US", "en_GB", "de_DE", "fr_FR"])  # Multi-locale for realistic mess
    Faker.seed(seed)
    random.seed(seed)
    return fake


def base_date() -> datetime:
    """The 'project epoch' — day 0 of our simulated retail company."""
    return datetime(2025, 1, 1, tzinfo=timezone.utc)


def simulated_now(day_offset: int) -> datetime:
    """What 'now' is, given how many days into the simulation we are."""
    return base_date() + timedelta(days=day_offset)


def write_jsonl(records: list[dict[str, Any]], path: str) -> None:
    """Write records as JSON Lines — one JSON object per line.
    
    JSONL is the standard for streaming/append-only data because each line
    is independently parseable. Auto Loader reads this natively.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def maybe_corrupt_json(line: str, corruption_rate: float = 0.001) -> str:
    """Inject malformed JSON at a low rate to test error handling.
    
    Real-world clickstream pipelines see this constantly due to truncated
    network packets, browser bugs, and ad-blockers mangling payloads.
    """
    if random.random() < corruption_rate:
        # Truncate randomly — simulates a partial write
        return line[: random.randint(10, len(line) - 1)]
    return line


def new_id() -> str:
    """Generate a UUID4 string — the standard for distributed systems."""
    return str(uuid.uuid4())
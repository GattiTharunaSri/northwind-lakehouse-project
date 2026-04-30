"""Clickstream event generator with out-of-order events and malformed JSON."""
from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

from .common import get_faker, new_id, simulated_now

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "search", "purchase", "wishlist_add"]
EVENT_WEIGHTS = [0.6, 0.12, 0.05, 0.15, 0.05, 0.03]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
OS_OPTIONS = ["iOS", "Android", "Windows", "macOS", "Linux"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]


def generate_clickstream_batch(day_offset: int, batch_size: int = 2_000) -> list[dict[str, Any]]:
    """Generate a batch of clickstream events.
    
    Includes:
    - ~5% out-of-order events (timestamp < arrival_time)
    - ~30% anonymous (no user_id)
    - Realistic event-type distribution skewed toward page_views
    """
    fake = get_faker(seed=day_offset * 31)
    base_ts = simulated_now(day_offset)
    events = []
    
    for _ in range(batch_size):
        is_out_of_order = random.random() < 0.05
        offset_secs = -random.randint(3600, 86_400) if is_out_of_order else random.randint(0, 86_400)
        event_ts = base_ts + timedelta(seconds=offset_secs)
        
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        has_user = random.random() > 0.30
        has_product = event_type in {"add_to_cart", "remove_from_cart", "purchase", "wishlist_add"} or random.random() < 0.4
        
        events.append({
            "event_id": new_id(),
            "user_id": f"CUST_{random.randint(0, 9999):06d}" if has_user else None,
            "session_id": f"SESS_{random.randint(0, 999_999):08d}",
            "event_type": event_type,
            "event_timestamp": event_ts.isoformat(),
            "page_url": f"https://northwind.example.com{fake.uri_path()}",
            "product_id": f"PROD_{random.randint(0, 999):05d}" if has_product else None,
            "device": {
                "type": random.choice(DEVICE_TYPES),
                "os": random.choice(OS_OPTIONS),
                "browser": random.choice(BROWSERS),
            },
            "geo": {
                "country": fake.country_code(),
                "region": fake.state_abbr(),
                "city": fake.city(),
            },
        })
    
    return events
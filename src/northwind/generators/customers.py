"""Customer data generator with weekly snapshots and address changes (SCD2 driver)."""
from __future__ import annotations

import random
from typing import Any

from .common import get_faker, simulated_now

LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
TIER_WEIGHTS = [0.5, 0.3, 0.15, 0.05]


def generate_customers_snapshot(day_offset: int, num_customers: int = 10_000) -> list[dict[str, Any]]:
    """Generate a weekly customer snapshot.
    
    Same customer_ids every week, but ~3% will have address/tier changes —
    these are the SCD2 events our Silver layer must capture.
    """
    fake = get_faker(seed=day_offset // 7)  # Weekly stability
    snapshot_date = simulated_now(day_offset).date()
    customers = []
    
    for i in range(num_customers):
        random.seed(i)  # Stable identity per customer
        first = fake.first_name()
        last = fake.last_name()
        
        # Address is mostly stable but changes ~3% per week
        random.seed(i + (day_offset // 7) * 100_000)
        address_changed = random.random() < 0.03
        random.seed(i if not address_changed else i * (day_offset // 7))
        
        customers.append({
            "customer_id": f"CUST_{i:06d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{i}@{fake.domain_name()}",
            "phone": fake.phone_number(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.postcode(),
                "country": "US",
            },
            "signup_date": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "loyalty_tier": random.choices(LOYALTY_TIERS, weights=TIER_WEIGHTS)[0],
            "preferred_language": random.choice(["en", "es", "fr", "de"]),
            "_snapshot_date": snapshot_date.isoformat(),
        })
    
    random.seed()
    return customers
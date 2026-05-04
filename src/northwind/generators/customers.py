"""Customer data generator with weekly snapshots and CONTROLLED address changes (SCD2 driver)."""
from __future__ import annotations

import hashlib
import random
from typing import Any

from .common import get_faker, simulated_now

LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
TIER_WEIGHTS = [0.5, 0.3, 0.15, 0.05]


def _stable_choice(customer_id: str, options: list, salt: str = "") -> Any:
    """Pick deterministically from options based on customer_id + salt.
    
    Same customer_id + same salt = same choice, every time, forever.
    No reliance on RNG state.
    """
    h = hashlib.md5(f"{customer_id}|{salt}".encode()).hexdigest()
    idx = int(h, 16) % len(options)
    return options[idx]


def _stable_address(customer_id: str, version: int = 0) -> dict:
    """Generate a deterministic address keyed by (customer_id, version).
    
    Version 0 = original address. Version 1 = first move. Etc.
    Same inputs always produce the same address.
    """
    # Use a per-customer Faker seeded from the customer_id + version
    h = int(hashlib.md5(f"{customer_id}|addr|v{version}".encode()).hexdigest(), 16)
    seed = h % (2**32)
    
    # Local Faker with deterministic state
    from faker import Faker
    local_fake = Faker("en_US")
    Faker.seed(seed)
    local_fake.seed_instance(seed)
    
    return {
        "street": local_fake.street_address(),
        "city": local_fake.city(),
        "state": local_fake.state_abbr(),
        "zip": local_fake.postcode(),
        "country": "US",
    }


def generate_customers_snapshot(
    day_offset: int,
    num_customers: int = 10_000,
    address_change_rate_per_week: float = 0.03,
) -> list[dict[str, Any]]:
    """Generate a weekly customer snapshot.
    
    Same customer_ids every week. Each week, ~3% of customers have a NEW
    address (versioned). The cumulative effect is realistic SCD2 churn:
    - Week 0: everyone has address v0
    - Week 1: ~3% advance to address v1
    - Week 2: ~3% MORE advance to next version
    """
    fake = get_faker(seed=42)
    week_num = day_offset // 7
    snapshot_date = simulated_now(day_offset).date()
    customers = []
    
    for i in range(num_customers):
        customer_id = f"CUST_{i:06d}"
        
        # Stable identity fields — derived from customer_id, never change
        first = _stable_choice(customer_id, [fake.first_name() for _ in range(50)], "first")
        last = _stable_choice(customer_id, [fake.last_name() for _ in range(50)], "last")
        signup_date = fake.date_between(start_date="-5y", end_date="-1y")
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        
        # ADDRESS VERSIONING:
        # For each week from 0 to current, flip a deterministic coin.
        # If True, advance the address version. This way, a customer who
        # changed in week 1 stays at v1 forever (until another change in a later week).
        address_version = 0
        for w in range(1, week_num + 1):
            # Deterministic per-customer-per-week "did you move this week?"
            coin = _stable_choice(
                customer_id,
                ["change", "stay", "stay", "stay", "stay", "stay", "stay", 
                 "stay", "stay", "stay", "stay", "stay", "stay", "stay", 
                 "stay", "stay", "stay", "stay", "stay", "stay", "stay", 
                 "stay", "stay", "stay", "stay", "stay", "stay", "stay", 
                 "stay", "stay", "stay", "stay", "stay"],  # 1/33 ≈ 3%
                salt=f"move_week_{w}",
            )
            if coin == "change":
                address_version += 1
        
        address = _stable_address(customer_id, version=address_version)
        
        # Tier — also versioned but with a different rate (we'll keep the existing churn pattern)
        tier_seed = int(hashlib.md5(f"{customer_id}|tier|w{week_num}".encode()).hexdigest(), 16)
        random.seed(tier_seed)
        # 95% of weeks, customer keeps their original tier; 5% chance of change
        if random.random() < 0.05:
            tier = random.choices(LOYALTY_TIERS, weights=TIER_WEIGHTS)[0]
        else:
            # Stable tier per customer
            tier = _stable_choice(customer_id, LOYALTY_TIERS * [4, 3, 2, 1].count(1) or LOYALTY_TIERS, "tier_base")
            # Simpler: just pick based on hash directly
            tier = LOYALTY_TIERS[int(hashlib.md5(f"{customer_id}|tier_base".encode()).hexdigest(), 16) % 4]
        
        # Email is also stable per identity
        email_seed = int(hashlib.md5(f"{customer_id}|email".encode()).hexdigest(), 16) % (2**32)
        from faker import Faker
        email_fake = Faker()
        Faker.seed(email_seed)
        email_fake.seed_instance(email_seed)
        email = f"{first.lower()}.{last.lower()}{i}@{email_fake.domain_name()}"
        phone_seed = int(hashlib.md5(f"{customer_id}|phone".encode()).hexdigest(), 16) % (2**32)
        phone_fake = Faker()
        phone_fake.seed_instance(phone_seed)
        phone = phone_fake.phone_number()
        
        customers.append({
            "customer_id": customer_id,
            "first_name": first,
            "last_name": last,
            "email": email,
            "phone": phone,
            "date_of_birth": dob.isoformat(),
            "address": address,
            "signup_date": signup_date.isoformat(),
            "loyalty_tier": tier,
            "preferred_language": _stable_choice(customer_id, ["en", "es", "fr", "de"], "lang"),
            "_snapshot_date": snapshot_date.isoformat(),
        })
    
    random.seed()
    return customers
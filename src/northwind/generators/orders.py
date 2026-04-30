"""Generate realistic order data with intentional production-grade issues."""
from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

from .common import get_faker, new_id, simulated_now

ORDER_STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
PAYMENT_METHODS = ["CARD", "PAYPAL", "BANK_TRANSFER", "GIFT_CARD"]
STATUS_WEIGHTS = [0.05, 0.15, 0.25, 0.45, 0.07, 0.03]  # Realistic distribution


def generate_order(
    fake,
    customer_pool: list[str],
    product_pool: list[dict],
    timestamp,
    inject_drift: bool = False,
) -> dict[str, Any]:
    """Generate a single order record."""
    
    # Pick 1-5 items per order (realistic basket size distribution)
    num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.05])[0]
    items = []
    total = 0.0
    for _ in range(num_items):
        product = random.choice(product_pool)
        qty = random.randint(1, 3)
        items.append({
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": product["price"],
        })
        total += qty * product["price"]
    
    # ~0.5% null customer_id — orphan orders happen when guest checkout fails
    customer_id = random.choice(customer_pool) if random.random() > 0.005 else None
    
    # ~0.1% negative total — system bug we want our DQ checks to catch
    if random.random() < 0.001:
        total = -total
    
    order = {
        "order_id": new_id(),
        "customer_id": customer_id,
        "order_date": timestamp.isoformat(),
        "status": random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0],
        "items": items,
        "total_amount": round(total, 2),
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr() if random.random() > 0.1 else fake.state(),
            "zip": fake.postcode(),
            "country": fake.country_code(),
        },
        "payment_method": random.choice(PAYMENT_METHODS),
        "_extracted_at": timestamp.isoformat(),
    }
    
    # SCHEMA DRIFT: starting day 5, source system adds discount_code field
    if inject_drift:
        order["discount_code"] = fake.bothify("???###").upper() if random.random() > 0.7 else None
    
    return order


def generate_orders_batch(
    day_offset: int,
    batch_size: int = 500,
    customer_pool: list[str] | None = None,
    product_pool: list[dict] | None = None,
    duplicate_rate: float = 0.01,
) -> list[dict]:
    """Generate a batch of orders for a given simulation day.
    
    Returns a list of order dicts, with intentional duplicates (~1%)
    to test our deduplication logic in Silver.
    """
    fake = get_faker(seed=day_offset)  # Same day = same data
    inject_drift = day_offset >= 5  # Schema drift kicks in on day 5
    
    if not customer_pool:
        customer_pool = [f"CUST_{i:06d}" for i in range(10_000)]
    if not product_pool:
        product_pool = [{"product_id": f"PROD_{i:05d}", "price": round(random.uniform(5, 500), 2)} for i in range(1_000)]
    
    base_ts = simulated_now(day_offset)
    orders = []
    
    for i in range(batch_size):
        # Spread orders across the day uniformly-ish
        ts = base_ts + timedelta(seconds=random.randint(0, 86_400))
        order = generate_order(fake, customer_pool, product_pool, ts, inject_drift)
        orders.append(order)
        
        # Inject duplicates: same order_id appears twice
        if random.random() < duplicate_rate:
            orders.append(order.copy())
    
    return orders
"""Product catalog generator with daily snapshots and price drift."""
from __future__ import annotations

import random
from typing import Any

from .common import get_faker, simulated_now

CATEGORIES = {
    "Electronics": ["Phones", "Laptops", "Audio", "Cameras", "Accessories"],
    "Clothing": ["Mens", "Womens", "Kids", "Shoes", "Accessories"],
    "Home": ["Furniture", "Kitchen", "Bedding", "Decor", "Storage"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Cycling", "Water"],
    "Books": ["Fiction", "Non-fiction", "Children", "Academic", "Comics"],
}
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Cyberdyne", "Tyrell"]


def generate_products_snapshot(day_offset: int, num_products: int = 1_000) -> list[dict[str, Any]]:
    """Generate a full product snapshot for a given day.
    
    Same product_ids appear every day, but ~5% of prices change to simulate
    a real catalog. This drives the SCD2 logic later.
    """
    fake = get_faker(seed=42)  # Products are stable across days, mostly
    snapshot_date = simulated_now(day_offset).date()
    products = []
    
    for i in range(num_products):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        
        # Base price seeded by product_id for stability
        random.seed(i)
        base_price = round(random.uniform(5, 500), 2)
        random.seed(day_offset * 1000 + i)  # Re-seed for daily variation
        
        # ~5% chance of price change on any given day
        price = round(base_price * random.uniform(0.85, 1.15), 2) if random.random() < 0.05 else base_price
        
        products.append({
            "product_id": f"PROD_{i:05d}",
            "product_name": f"{random.choice(BRANDS)} {fake.word().title()} {subcategory[:-1]}",
            "category": category,
            "subcategory": subcategory,
            "brand": random.choice(BRANDS),
            "price": price,
            "cost": round(price * 0.6, 2),
            "weight_kg": round(random.uniform(0.1, 20), 2),
            "is_active": random.random() > 0.02,  # 2% inactive
            "last_updated": snapshot_date.isoformat(),
        })
    
    random.seed()  # Reset
    return products
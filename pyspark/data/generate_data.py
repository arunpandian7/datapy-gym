"""
Generate synthetic e-commerce dataset for the PySpark coding gym.
Run once with: uv run python pyspark/data/generate_data.py
"""

import random
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── Reproducibility ──────────────────────────────────────────────────────────
np.random.seed(42)
random.seed(42)

OUTPUT_DIR = Path(__file__).parent

# ── Helper ───────────────────────────────────────────────────────────────────

def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


# ── customers.csv ─────────────────────────────────────────────────────────────

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Seattle", "Denver", "Nashville",
]

COUNTRY_CHOICES = ["US"] * 80 + ["CA"] * 10 + ["UK"] * 6 + ["AU"] * 4  # 100-element pool

TIER_CHOICES = (
    ["bronze"] * 40 + ["silver"] * 30 + ["gold"] * 20 + ["platinum"] * 10
)

NUM_CUSTOMERS = 500
SIGNUP_START = date(2020, 1, 1)
SIGNUP_END = date(2023, 6, 30)

customers_rows = []
for cid in range(1, NUM_CUSTOMERS + 1):
    customers_rows.append(
        {
            "customer_id": cid,
            "name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "email": f"user{cid}@example.com",
            "city": random.choice(CITIES),
            "country": random.choice(COUNTRY_CHOICES),
            "tier": random.choice(TIER_CHOICES),
            "signup_date": random_date(SIGNUP_START, SIGNUP_END),
        }
    )

df_customers = pd.DataFrame(customers_rows)

# ── products.csv ──────────────────────────────────────────────────────────────

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports",
    "Toys",
    "Beauty",
    "Food & Beverage",
    "Automotive",
    "Office Supplies",
]

BRANDS = {
    "Electronics": "TechCore",
    "Clothing": "UrbanThread",
    "Books": "PageTurner",
    "Home & Garden": "NestCraft",
    "Sports": "PeakGear",
    "Toys": "FunZone",
    "Beauty": "GlowUp",
    "Food & Beverage": "PureTaste",
    "Automotive": "DriveWell",
    "Office Supplies": "DeskPro",
}

PRODUCTS_PER_CATEGORY = 10
NUM_PRODUCTS = len(CATEGORIES) * PRODUCTS_PER_CATEGORY  # 100

products_rows = []
pid = 1
for cat in CATEGORIES:
    brand = BRANDS[cat]
    for n in range(1, PRODUCTS_PER_CATEGORY + 1):
        price = round(random.uniform(5.99, 499.99), 2)
        products_rows.append(
            {
                "product_id": pid,
                "name": f"{brand} {cat} Item {n}",
                "category": cat,
                "brand": brand,
                "price": price,
            }
        )
        pid += 1

df_products = pd.DataFrame(products_rows)

# ── order_items.csv + orders.csv ──────────────────────────────────────────────

# Build price lookup once
product_price_map = dict(zip(df_products["product_id"], df_products["price"]))
product_ids = df_products["product_id"].tolist()

NUM_ORDERS = 8000
SKEW_CUSTOMER = 1
SKEW_COUNT = 2400          # 30 % of 8000
REST_COUNT = NUM_ORDERS - SKEW_COUNT  # 5600

ORDER_START = date(2023, 1, 1)
ORDER_END = date(2024, 12, 31)

STATUS_POOL = (
    ["completed"] * 60
    + ["pending"] * 15
    + ["cancelled"] * 15
    + ["refunded"] * 10
)

# Build shuffled customer_id assignment list
customer_ids_assignment = [SKEW_CUSTOMER] * SKEW_COUNT + list(
    np.random.choice(range(2, NUM_CUSTOMERS + 1), size=REST_COUNT, replace=True)
)
random.shuffle(customer_ids_assignment)

# Generate items first, accumulate order totals
all_items = []       # list of dicts for order_items
order_totals = {}    # order_id -> total_amount

item_id = 1
for order_id in range(1, NUM_ORDERS + 1):
    num_items = random.randint(1, 4)
    chosen_products = np.random.choice(product_ids, size=num_items, replace=False)
    order_total = 0.0
    for prod_id in chosen_products:
        qty = random.randint(1, 5)
        unit_price = product_price_map[int(prod_id)]
        all_items.append(
            {
                "item_id": item_id,
                "order_id": order_id,
                "product_id": int(prod_id),
                "quantity": qty,
                "unit_price": unit_price,
            }
        )
        order_total += qty * unit_price
        item_id += 1
    order_totals[order_id] = round(order_total, 2)

df_order_items = pd.DataFrame(all_items)

# Build orders DataFrame
orders_rows = []
for order_id in range(1, NUM_ORDERS + 1):
    orders_rows.append(
        {
            "order_id": order_id,
            "customer_id": customer_ids_assignment[order_id - 1],
            "order_date": random_date(ORDER_START, ORDER_END),
            "status": random.choice(STATUS_POOL),
            "total_amount": order_totals[order_id],
        }
    )

df_orders = pd.DataFrame(orders_rows)

# ── Write CSVs ────────────────────────────────────────────────────────────────

df_customers.to_csv(OUTPUT_DIR / "customers.csv", index=False)
df_products.to_csv(OUTPUT_DIR / "products.csv", index=False)
df_orders.to_csv(OUTPUT_DIR / "orders.csv", index=False)
df_order_items.to_csv(OUTPUT_DIR / "order_items.csv", index=False)

# ── Summary ───────────────────────────────────────────────────────────────────

skew_orders = (df_orders["customer_id"] == SKEW_CUSTOMER).sum()
skew_pct = skew_orders / len(df_orders) * 100

print(f"customers:    {len(df_customers):>5} rows  →  customers.csv")
print(f"products:     {len(df_products):>5} rows  →  products.csv")
print(f"orders:       {len(df_orders):>5} rows  →  orders.csv")
print(f"order_items: {len(df_order_items):>6} rows  →  order_items.csv")
print()
print(
    f"Skew check: customer_id={SKEW_CUSTOMER} has {skew_orders} orders "
    f"({skew_pct:.1f}% of total)"
)

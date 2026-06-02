def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [

        # ── Title ────────────────────────────────────────────────────────────
        ("markdown", """\
# PySpark Gym — 03: Joins

Practice: inner / left / anti / semi joins, multi-table joins, broadcast hints, and self-joins.
Each problem builds a result DataFrame; assign it to the named `solution_N` variable and run the check cell.\
"""),

        # ── Setup ────────────────────────────────────────────────────────────
        ("code", """\
from pathlib import Path
import sys

_cwd = Path.cwd()
_candidates = [_cwd / "pyspark", _cwd, _cwd.parent / "pyspark", _cwd.parent.parent / "pyspark"]
_pyspark_dir = next((p for p in _candidates if (p / "utils" / "__init__.py").exists()), None)
if _pyspark_dir is None:
    raise RuntimeError("Cannot locate pyspark/utils. Run: uv run jupyter lab from the project root.")

if str(_pyspark_dir) not in sys.path:
    sys.path.insert(0, str(_pyspark_dir))

DATA_DIR = _pyspark_dir / "data"

from utils import get_spark, check
import pyspark.sql.functions as F
from pyspark.sql import Window

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

customers   = spark.read.csv(str(DATA_DIR / "customers.csv"),   header=True, inferSchema=True)
products    = spark.read.csv(str(DATA_DIR / "products.csv"),    header=True, inferSchema=True)
orders      = spark.read.csv(str(DATA_DIR / "orders.csv"),      header=True, inferSchema=True)
order_items = spark.read.csv(str(DATA_DIR / "order_items.csv"), header=True, inferSchema=True)

for df in [customers, products, orders, order_items]: df.cache()

print(f"customers:   {customers.count():>6,}")
print(f"products:    {products.count():>6,}")
print(f"orders:      {orders.count():>6,}")
print(f"order_items: {order_items.count():>6,}")\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 1: Enriched Order Line Items

Build a fully-enriched line-item table by joining all four source tables, then filter to completed orders only.

**Approach:** Join `order_items` → `orders` (on `order_id`) → `products` (on `product_id`) → `customers` (on `customer_id`).
Alias `products.name` to `product_name` and `customers.name` to `customer_name` before or during the join to avoid
ambiguity. Compute `line_total` as `round(quantity * unit_price, 2)`.

| Column | Type | Notes |
|--------|------|-------|
| order_id | int | |
| order_date | date | |
| customer_name | string | from `customers.name` |
| tier | string | bronze / silver / gold / platinum |
| product_name | string | from `products.name` |
| category | string | |
| quantity | int | |
| unit_price | double | |
| line_total | double | `round(quantity * unit_price, 2)` |

Expected: all completed-order line items, unordered.\
"""),

        ("code", """\
solution_1 = None  # ← your answer here\
"""),

        ("code", """\
_products_1 = products.withColumnRenamed("name", "product_name")
_customers_1 = customers.withColumnRenamed("name", "customer_name")

_expected_1 = (
    order_items
    .join(orders, "order_id")
    .filter(F.col("status") == "completed")
    .join(_products_1, "product_id")
    .join(_customers_1, "customer_id")
    .select(
        "order_id",
        "order_date",
        "customer_name",
        "tier",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        F.round(F.col("quantity") * F.col("unit_price"), 2).alias("line_total"),
    )
)
check(solution_1, _expected_1, problem="P1: Enriched Order Line Items", ordered=False)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 2
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 2: Customers Who Have Never Ordered

Find customers with no record in the orders table — a classic anti-join pattern.

**Approach:** Build the set of `customer_id` values that appear in `orders` (use `.select("customer_id").distinct()`),
then do a **left anti join** from `customers` against that set. Anti join keeps only left-side rows that
have *no* matching key on the right.

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| name | string | |
| email | string | |
| tier | string | |

Expected: all customers absent from `orders`, sorted by `customer_id` ASC.\
"""),

        ("code", """\
solution_2 = None  # ← your answer here\
"""),

        ("code", """\
_ordered_customers_2 = orders.select("customer_id").distinct()
_expected_2 = (
    customers
    .join(_ordered_customers_2, on="customer_id", how="left_anti")
    .select("customer_id", "name", "email", "tier")
    .orderBy("customer_id")
)
check(solution_2, _expected_2, problem="P2: Customers Who Have Never Ordered", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 3
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 3: Most Frequently Bought Product Pairs

Find the top 10 pairs of products that appear together most often in the same order.

**Approach:** Self-join `order_items` as aliases `"a"` and `"b"` on `order_id`, filter
`a.product_id < b.product_id` (avoids duplicate pairs and self-pairs), then count.
Use `.alias("a")` / `.alias("b")` and reference columns with `F.col("a.product_id")`.

| Column | Type | Notes |
|--------|------|-------|
| product_id_1 | int | the smaller product_id in the pair |
| product_id_2 | int | the larger product_id in the pair |
| times_bought_together | long | sorted DESC, top 10 |

Expected: 10 rows, ordered by `times_bought_together` DESC.\
"""),

        ("code", """\
solution_3 = None  # ← your answer here\
"""),

        ("code", """\
_a_3 = order_items.alias("a")
_b_3 = order_items.alias("b")
_expected_3 = (
    _a_3
    .join(_b_3, on="order_id")
    .filter(F.col("a.product_id") < F.col("b.product_id"))
    .groupBy(
        F.col("a.product_id").alias("product_id_1"),
        F.col("b.product_id").alias("product_id_2"),
    )
    .agg(F.count("*").alias("times_bought_together"))
    .orderBy(F.col("times_bought_together").desc())
    .limit(10)
)
check(solution_3, _expected_3, problem="P3: Most Frequently Bought Product Pairs", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 4: Customers Who Bought From Both Electronics AND Sports

Find customers who have purchased at least one item from the **Electronics** category *and*
at least one item from the **Sports** category.

**Approach:**
1. Join `order_items` with `products` to get category per line item.
2. Join with `orders` to get `customer_id` per line item.
3. Filter to Electronics; take distinct `customer_id` → call it `elec_customers`.
4. Filter to Sports; take distinct `customer_id` → call it `sport_customers`.
5. Inner join the two sets on `customer_id` — only IDs present in *both* survive.
6. Join back to `customers` to get `name`.

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| name | string | |

Expected: customers present in both category buyer sets, sorted by `customer_id` ASC.\
"""),

        ("code", """\
solution_4 = None  # ← your answer here\
"""),

        ("code", """\
_items_with_cat_4 = (
    order_items
    .join(products, "product_id")
    .join(orders.select("order_id", "customer_id"), "order_id")
)
_elec_4 = _items_with_cat_4.filter(F.col("category") == "Electronics").select("customer_id").distinct()
_sport_4 = _items_with_cat_4.filter(F.col("category") == "Sports").select("customer_id").distinct()
_expected_4 = (
    _elec_4
    .join(_sport_4, "customer_id")
    .join(customers.select("customer_id", "name"), "customer_id")
    .orderBy("customer_id")
)
check(solution_4, _expected_4, problem="P4: Customers Who Bought From Both Electronics AND Sports", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 5
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 5: Revenue Share by Customer Tier

Show how much each customer tier contributes to total revenue as a percentage.

**Approach:** Join `orders` with `customers` on `customer_id`, group by `tier`, sum
`total_amount` → `tier_revenue`. Collect the grand total with
`orders.agg(F.sum("total_amount")).first()[0]` and divide each tier's revenue by it
to get `revenue_share_pct`.

| Column | Type | Notes |
|--------|------|-------|
| tier | string | sorted ASC |
| tier_revenue | double | `round(sum(total_amount), 2)` |
| revenue_share_pct | double | `round(tier_revenue / grand_total * 100, 2)` |

Expected: one row per tier, sorted by `tier` ASC.\
"""),

        ("code", """\
solution_5 = None  # ← your answer here\
"""),

        ("code", """\
_total_5 = orders.agg(F.sum("total_amount")).first()[0]
_expected_5 = (
    orders
    .join(customers, "customer_id")
    .groupBy("tier")
    .agg(F.round(F.sum("total_amount"), 2).alias("tier_revenue"))
    .withColumn("revenue_share_pct", F.round(F.col("tier_revenue") / _total_5 * 100, 2))
    .orderBy("tier")
)
check(solution_5, _expected_5, problem="P5: Revenue Share by Customer Tier", ordered=True)\
"""),

    ]

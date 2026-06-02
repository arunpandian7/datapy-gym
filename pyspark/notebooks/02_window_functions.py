def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [

        # ── Title ────────────────────────────────────────────────────────────
        ("markdown", """\
# PySpark Gym — 02: Window Functions

Practice: ranking (`dense_rank`, `row_number`), running aggregations, `lag`/`lead`,
and `percent_rank` — all using `pyspark.sql.Window`.
Each problem builds a result DataFrame; assign it to the named `solution_N` variable and run the check cell.\
"""),

        # ── Setup ────────────────────────────────────────────────────────────
        ("code", """\
from pathlib import Path
import sys

# Find pyspark/ directory regardless of where jupyter was launched from
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

        # ── Data preview ─────────────────────────────────────────────────────
        ("code", """\
for name, df in [("orders", orders), ("order_items", order_items),
                 ("customers", customers), ("products", products)]:
    print(f"\\n{'─'*50}\\n  {name}\\n{'─'*50}")
    df.printSchema()
    df.show(3, truncate=False)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 1: Top 3 Products by Revenue Within Each Category

Rank products by revenue inside each category using `dense_rank`, then keep the top 3.

**Approach hint:** First aggregate to `(product_id, name, category, revenue)`, then apply
`dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC)`. Filter `rank <= 3`.

| Column | Type | Notes |
|--------|------|-------|
| category | string | |
| name | string | product name |
| revenue | double | `round(sum(quantity * unit_price), 2)` |
| rank | int | dense rank within category, 1 = highest revenue |

Expected: up to 3 rows per category (ties keep all), ordered `category` ASC, `rank` ASC.\
"""),

        ("code", """\
solution_1 = None  # ← your answer here\
"""),

        ("code", """\
_joined_1 = order_items.join(products, "product_id")
_agg_1 = (
    _joined_1
    .groupBy("product_id", F.col("name"), "category")
    .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue"))
)
_w1 = Window.partitionBy("category").orderBy(F.col("revenue").desc())
_expected_1 = (
    _agg_1
    .withColumn("rank", F.dense_rank().over(_w1))
    .filter(F.col("rank") <= 3)
    .select("category", "name", "revenue", "rank")
    .orderBy("category", "rank")
)
check(solution_1, _expected_1, problem="P1: Top 3 Products by Revenue Within Each Category", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 2
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 2: Cumulative Daily Revenue

Compute a running total of revenue over time — useful for tracking how quickly revenue
accumulates across the year.

**Approach hint:** First aggregate `orders` to daily revenue, then apply
`sum(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

| Column | Type | Notes |
|--------|------|-------|
| order_date | date | |
| daily_revenue | double | `round(sum(total_amount), 2)` for that day |
| running_total | double | `round(cumulative sum of daily_revenue, 2)` |

Expected: one row per day, ordered `order_date` ASC.\
"""),

        ("code", """\
solution_2 = None  # ← your answer here\
"""),

        ("code", """\
_daily_2 = (
    orders
    .groupBy("order_date")
    .agg(F.round(F.sum("total_amount"), 2).alias("daily_revenue"))
)
_w2 = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
_expected_2 = (
    _daily_2
    .withColumn("running_total", F.round(F.sum("daily_revenue").over(_w2), 2))
    .orderBy("order_date")
)
check(solution_2, _expected_2, problem="P2: Cumulative Daily Revenue", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 3
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 3: Month-over-Month Revenue Change

Compare each month's revenue to the previous month and compute the percentage change.

**Approach hint:** Aggregate to monthly revenue, then use
`lag(monthly_revenue, 1) OVER (ORDER BY month)` to get the prior month's value.
The first row will have `null` for `prev_revenue` and `mom_change_pct`.

| Column | Type | Notes |
|--------|------|-------|
| month | string | `"yyyy-MM"` format |
| monthly_revenue | double | `round(sum(total_amount), 2)` |
| prev_revenue | double | prior month's revenue (null for first row) |
| mom_change_pct | double | `round((monthly_revenue - prev_revenue) / prev_revenue * 100, 2)` |

Expected: one row per month, ordered `month` ASC.\
"""),

        ("code", """\
solution_3 = None  # ← your answer here\
"""),

        ("code", """\
_monthly_3 = (
    orders
    .withColumn("month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("month")
    .agg(F.round(F.sum("total_amount"), 2).alias("monthly_revenue"))
)
_w3 = Window.orderBy("month")
_expected_3 = (
    _monthly_3
    .withColumn("prev_revenue", F.lag("monthly_revenue", 1).over(_w3))
    .withColumn(
        "mom_change_pct",
        F.round((F.col("monthly_revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 2),
    )
    .orderBy("month")
)
check(solution_3, _expected_3, problem="P3: Month-over-Month Revenue Change", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 4: Top 2 Customers per Tier by Spend (Completed Orders Only)

Within each customer tier, find the two highest-spending customers — useful for tier-based
loyalty targeting.

**Approach hint:** Filter to `status == "completed"`, join `customers`, aggregate spend,
then apply `row_number() OVER (PARTITION BY tier ORDER BY total_spend DESC)`. Filter `rank_in_tier <= 2`.

| Column | Type | Notes |
|--------|------|-------|
| tier | string | bronze / silver / gold / platinum |
| customer_id | int | |
| name | string | customer name |
| total_spend | double | `round(sum(total_amount), 2)` |
| rank_in_tier | int | 1 = top spender in that tier |

Expected: up to 2 rows per tier, ordered `tier` ASC, `rank_in_tier` ASC.\
"""),

        ("code", """\
solution_4 = None  # ← your answer here\
"""),

        ("code", """\
_completed_4 = orders.filter(F.col("status") == "completed")
_agg_4 = (
    _completed_4
    .join(customers, "customer_id")
    .groupBy("customer_id", "name", "tier")
    .agg(F.round(F.sum("total_amount"), 2).alias("total_spend"))
)
_w4 = Window.partitionBy("tier").orderBy(F.col("total_spend").desc())
_expected_4 = (
    _agg_4
    .withColumn("rank_in_tier", F.row_number().over(_w4))
    .filter(F.col("rank_in_tier") <= 2)
    .select("tier", "customer_id", "name", "total_spend", "rank_in_tier")
    .orderBy("tier", "rank_in_tier")
)
check(solution_4, _expected_4, problem="P4: Top 2 Customers per Tier by Spend (Completed Orders Only)", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 5
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 5: Percentile Rank of Order Amounts Within Each Status

For each order, compute where it falls in the distribution of order amounts within its own
status group — e.g. a "completed" order at the 90th percentile spent more than 90% of
other completed orders.

**Approach hint:** Apply `percent_rank() OVER (PARTITION BY status ORDER BY total_amount)`
directly on the `orders` DataFrame — no pre-aggregation needed.

| Column | Type | Notes |
|--------|------|-------|
| order_id | int | |
| status | string | completed / pending / cancelled / refunded |
| total_amount | double | original order amount |
| pct_rank | double | `round(percent_rank(), 4)`, 0.0 = lowest, 1.0 = highest |

Expected: all orders, ordered `status` ASC, `total_amount` ASC.\
"""),

        ("code", """\
solution_5 = None  # ← your answer here\
"""),

        ("code", """\
_w5 = Window.partitionBy("status").orderBy("total_amount")
_expected_5 = (
    orders
    .withColumn("pct_rank", F.round(F.percent_rank().over(_w5), 4))
    .select("order_id", "status", "total_amount", "pct_rank")
    .orderBy("status", "total_amount")
)
check(solution_5, _expected_5, problem="P5: Percentile Rank of Order Amounts Within Each Status", ordered=True)\
"""),

    ]

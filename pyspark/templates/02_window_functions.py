def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# PySpark Gym — 02: Window Functions

Practice: ranking (`dense_rank`, `row_number`), running aggregations, `lag`/`lead`,
and `percent_rank` — all using `pyspark.sql.Window`.
Each problem builds a result DataFrame; assign it to the named `solution_N` variable and run the check cell.\
""",
        ),
        # ── Setup ────────────────────────────────────────────────────────────
        (
            "code",
            """\
from pathlib import Path
import sys

# Find pyspark/ directory regardless of where jupyter was launched from
_cwd = Path.cwd()
_candidates = [_cwd / "pyspark", _cwd, _cwd.parent, _cwd.parent / "pyspark", _cwd.parent.parent, _cwd.parent.parent / "pyspark"]
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

from utils.checks.window_functions import Checker
checker = Checker(spark, customers, products, orders, order_items)\

from utils.checks.window_functions import Checker
checker = Checker(spark, customers, products, orders, order_items)\
""",
        ),
        # ── Data preview ─────────────────────────────────────────────────────
        (
            "code",
            """\
for name, df in [("orders", orders), ("order_items", order_items),
                 ("customers", customers), ("products", products)]:
    print(f"\\n{'─'*50}\\n  {name}\\n{'─'*50}")
    df.printSchema()
    df.show(3, truncate=False)\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 1: Top 3 Products by Revenue Within Each Category

Rank products by revenue inside each category using `dense_rank`, then keep the top 3.

<details>
<summary>Hint</summary>

First aggregate to `(product_id, name, category, revenue)`, then apply
`dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC)`. Filter `rank <= 3`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| category | string | |
| name | string | product name |
| revenue | double | `round(sum(quantity * unit_price), 2)` |
| rank | int | dense rank within category, 1 = highest revenue |

Expected: up to 3 rows per category (ties keep all), ordered `category` ASC, `rank` ASC.\
""",
        ),
        (
            "code",
            """\
solution_1 = None  # ← your answer here\
""",
        ),
        ("code", "checker.p1(solution_1)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 2
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 2: Cumulative Daily Revenue

Compute a running total of revenue over time — useful for tracking how quickly revenue
accumulates across the year.

<details>
<summary>Hint</summary>

First aggregate `orders` to daily revenue, then apply
`sum(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| order_date | date | |
| daily_revenue | double | `round(sum(total_amount), 2)` for that day |
| running_total | double | `round(cumulative sum of daily_revenue, 2)` |

Expected: one row per day, ordered `order_date` ASC.\
""",
        ),
        (
            "code",
            """\
solution_2 = None  # ← your answer here\
""",
        ),
        ("code", "checker.p2(solution_2)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 3
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 3: Month-over-Month Revenue Change

Compare each month's revenue to the previous month and compute the percentage change.

<details>
<summary>Hint</summary>

Aggregate to monthly revenue, then use
`lag(monthly_revenue, 1) OVER (ORDER BY month)` to get the prior month's value.
The first row will have `null` for `prev_revenue` and `mom_change_pct`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| month | string | `"yyyy-MM"` format |
| monthly_revenue | double | `round(sum(total_amount), 2)` |
| prev_revenue | double | prior month's revenue (null for first row) |
| mom_change_pct | double | `round((monthly_revenue - prev_revenue) / prev_revenue * 100, 2)` |

Expected: one row per month, ordered `month` ASC.\
""",
        ),
        (
            "code",
            """\
solution_3 = None  # ← your answer here\
""",
        ),
        ("code", "checker.p3(solution_3)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 4: Top 2 Customers per Tier by Spend (Completed Orders Only)

Within each customer tier, find the two highest-spending customers — useful for tier-based
loyalty targeting.

<details>
<summary>Hint</summary>

Filter to `status == "completed"`, join `customers`, aggregate spend,
then apply `row_number() OVER (PARTITION BY tier ORDER BY total_spend DESC)`. Filter `rank_in_tier <= 2`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| tier | string | bronze / silver / gold / platinum |
| customer_id | int | |
| name | string | customer name |
| total_spend | double | `round(sum(total_amount), 2)` |
| rank_in_tier | int | 1 = top spender in that tier |

Expected: up to 2 rows per tier, ordered `tier` ASC, `rank_in_tier` ASC.\
""",
        ),
        (
            "code",
            """\
solution_4 = None  # ← your answer here\
""",
        ),
        ("code", "checker.p4(solution_4)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 5
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 5: Percentile Rank of Order Amounts Within Each Status

For each order, compute where it falls in the distribution of order amounts within its own
status group — e.g. a "completed" order at the 90th percentile spent more than 90% of
other completed orders.

<details>
<summary>Hint</summary>

Apply `percent_rank() OVER (PARTITION BY status ORDER BY total_amount)`
directly on the `orders` DataFrame — no pre-aggregation needed.

</details>

| Column | Type | Notes |
|--------|------|-------|
| order_id | int | |
| status | string | completed / pending / cancelled / refunded |
| total_amount | double | original order amount |
| pct_rank | double | `round(percent_rank(), 4)`, 0.0 = lowest, 1.0 = highest |

Expected: all orders, ordered `status` ASC, `total_amount` ASC.\
""",
        ),
        (
            "code",
            """\
solution_5 = None  # ← your answer here\
""",
        ),
        ("code", "checker.p5(solution_5)"),
    ]

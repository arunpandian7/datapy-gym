def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# PySpark Gym — 08: Sessionization & Slowly Changing Dimensions

Practice: gap-based event sessionization with window functions, and building a
Slowly Changing Dimension Type 2 (SCD2) table from a change feed — two patterns that
show up constantly in clickstream analytics and dimensional warehousing.\
""",
        ),
        # ── Background ───────────────────────────────────────────────────────
        (
            "markdown",
            """\
## Sessionization and SCD2, briefly

**Sessionization** groups a customer's events (here, orders) into sessions separated by
a gap threshold — if more than `GAP_DAYS` pass between two consecutive orders, a new
session starts. The standard pattern: `LAG` to find the previous timestamp, a boolean
flag for "gap exceeded", then a running `SUM` over that flag to assign monotonically
increasing session IDs.

**SCD Type 2** preserves history when a dimension attribute changes (e.g. a customer's
loyalty tier). Instead of overwriting the row, you close out the old version with an
`effective_end` date and insert a new version with `effective_start` = the change date
and `effective_end = NULL`. A `is_current` flag marks the live row. This notebook
simulates a "Day 2" change feed against the existing `customers` table for the first 10
customers.\
""",
        ),
        # ── Setup ────────────────────────────────────────────────────────────
        (
            "code",
            """\
from pathlib import Path
import sys

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

customers   = spark.read.csv(str(DATA_DIR / "customers.csv"),   header=True, inferSchema=True).withColumn("signup_date", F.to_date("signup_date"))
products    = spark.read.csv(str(DATA_DIR / "products.csv"),    header=True, inferSchema=True)
orders      = spark.read.csv(str(DATA_DIR / "orders.csv"),      header=True, inferSchema=True).withColumn("order_date", F.to_date("order_date"))
order_items = spark.read.csv(str(DATA_DIR / "order_items.csv"), header=True, inferSchema=True)

for df in [customers, products, orders, order_items]: df.cache()

print(f"customers:   {customers.count():>6,}")
print(f"products:    {products.count():>6,}")
print(f"orders:      {orders.count():>6,}")
print(f"order_items: {order_items.count():>6,}")

GAP_DAYS = 30

# Synthetic "Day 2" change feed for SCD2 problems — new tier per customer_id 1-10
UPDATES_DATA = [
    (1, "silver"), (2, "gold"), (3, "gold"), (4, "bronze"), (5, "platinum"),
    (6, "bronze"), (7, "silver"), (8, "silver"), (9, "bronze"), (10, "gold"),
]
CHANGE_DATE = "2024-01-15"
updates_feed = spark.createDataFrame(UPDATES_DATA, ["customer_id", "new_tier"]) \\
    .withColumn("change_date", F.lit(CHANGE_DATE).cast("date"))
baseline = customers.filter(F.col("customer_id").between(1, 10)).select("customer_id", "tier", "signup_date")

from utils.checks.sessionization_scd import Checker
checker = Checker(spark, customers, products, orders, order_items)\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 1: Gap-Based Sessionization — Assign Session IDs

For each customer, assign a `session_id` to every order: a new session starts whenever
more than `GAP_DAYS` (30) pass since that customer's previous order.

<details>
<summary>Hint</summary>

```python
w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
(
    orders
    .withColumn("prev_date", F.lag("order_date").over(w))
    .withColumn("gap_days", F.datediff("order_date", "prev_date"))
    .withColumn("is_new_session",
                F.when((F.col("prev_date").isNull()) | (F.col("gap_days") > GAP_DAYS), 1).otherwise(0))
    .withColumn("session_id", F.sum("is_new_session").over(w))
)
```

The first order for each customer has no `prev_date`, so it always starts session 1.
A running `sum` over the "new session" flag turns 0/1 flags into a monotonically
increasing session counter.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| order_id | int | |
| order_date | date | sorted ASC within customer |
| session_id | long | starts at 1 per customer |

Expected: 8 000 rows (one per order).\
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
## Problem 2: Per-Session Summary — Start, End, Count, Revenue

Using the session IDs from Problem 1, roll each session up into a single summary row.

<details>
<summary>Hint</summary>

Rebuild the sessioned DataFrame from P1, then
`groupBy("customer_id", "session_id")` and aggregate `min("order_date")` as
`session_start`, `max("order_date")` as `session_end`, `count("order_id")` as
`order_count`, and `round(sum("total_amount"), 2)` as `session_revenue`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| session_id | long | sorted ASC within customer |
| session_start | date | |
| session_end | date | |
| order_count | long | |
| session_revenue | double | `round(sum(total_amount), 2)` |

Expected: one row per (customer, session) pair.\
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
## Problem 3: Detect Changed Customers (CDC Diff)

Compare the `updates_feed` (the incoming "Day 2" snapshot) against `baseline` (today's
dimension) and find which customers actually changed tier. Unchanged rows should be
dropped — this is the diff step before any SCD2 logic runs.

<details>
<summary>Hint</summary>

`updates_feed.join(baseline, "customer_id").filter(F.col("new_tier") != F.col("tier"))`.
Select `customer_id`, the old tier (aliased `old_tier`), `new_tier`, and `change_date`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| old_tier | string | tier before the change |
| new_tier | string | tier after the change |
| change_date | date | 2024-01-15 for every row |

Expected: 5 rows — only customers whose tier actually differs between baseline and feed.\
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
## Problem 4: Build the SCD Type 2 Dimension

Using the changes detected in Problem 3, build a full SCD2 table for customers 1–10:
changed customers get **two** rows (an expired old version and a new current version);
unchanged customers keep their **single** row, still current.

<details>
<summary>Hint</summary>

Three pieces, unioned together:

1. **Expired rows** — for changed customers: `tier` = old tier,
   `effective_start` = `signup_date`, `effective_end` = `change_date - 1 day`
   (`F.date_sub(change_date, 1)`), `is_current = False`.
2. **New current rows** — for changed customers: `tier` = `new_tier`,
   `effective_start` = `change_date`, `effective_end = NULL`, `is_current = True`.
3. **Untouched rows** — for customers *not* in the changes set (`left_anti` join):
   `tier` = baseline tier, `effective_start` = `signup_date`, `effective_end = NULL`,
   `is_current = True`.

`unionByName` the three pieces together.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| tier | string | |
| effective_start | date | |
| effective_end | date \\| null | null for the current version |
| is_current | boolean | |

Expected: 15 rows total — 10 customers, 5 of whom have 2 versions.\
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
## Problem 5: Point-in-Time Lookup Against the SCD2 Table

Given a set of (customer, as_of_date) pairs, look up what tier each customer was
classified as **at that point in time**, using the SCD2 table from Problem 4.

<details>
<summary>Hint</summary>

```python
asof_dates = spark.createDataFrame(
    [(1, "2024-01-10"), (1, "2024-02-01"), (5, "2024-01-10"),
     (5, "2024-02-01"), (4, "2024-02-01")],
    ["customer_id", "as_of_date"],
).withColumn("as_of_date", F.col("as_of_date").cast("date"))
```

Rebuild your P4 table (or reuse it), then join on `customer_id` and filter:
`as_of_date >= effective_start AND (effective_end IS NULL OR as_of_date <= effective_end)`.
This is the standard "as-of" join pattern for any SCD2 table.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| as_of_date | date | sorted ASC within customer |
| tier_as_of | string | the tier that was effective on that date |

Expected: 5 rows. Customer 1 should show `bronze` on 2024-01-10 (before the change) and
`silver` on 2024-02-01 (after).\
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

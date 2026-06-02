def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# PySpark Gym — 04: Partitioning

Practice: `repartition` vs `coalesce`, partition-by-column, `repartitionByRange`,
shuffle partition control via `spark.sql.shuffle.partitions`, and `spark_partition_id()`.
Each problem asks you to manipulate partitioning and return a verifiable DataFrame.\
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

customers   = spark.read.csv(str(DATA_DIR / "customers.csv"),   header=True, inferSchema=True)
products    = spark.read.csv(str(DATA_DIR / "products.csv"),    header=True, inferSchema=True)
orders      = spark.read.csv(str(DATA_DIR / "orders.csv"),      header=True, inferSchema=True)
order_items = spark.read.csv(str(DATA_DIR / "order_items.csv"), header=True, inferSchema=True)

for df in [customers, products, orders, order_items]: df.cache()

print(f"customers:   {customers.count():>6,}")
print(f"products:    {products.count():>6,}")
print(f"orders:      {orders.count():>6,}")
print(f"order_items: {order_items.count():>6,}")\

from utils.checks.partitioning import Checker
checker = Checker(spark, customers, products, orders, order_items)\

from utils.checks.partitioning import Checker
checker = Checker(spark, customers, products, orders, order_items)\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 1: Row Distribution After repartition(4)

Repartition `orders` into exactly 4 partitions, then measure how many rows landed in each partition.

**Approach:** Call `.repartition(4)` on `orders`. Add a `partition_id` column with
`F.spark_partition_id()`, group by it, and count rows.

| Column | Type | Notes |
|--------|------|-------|
| partition_id | int | 0–3 |
| row_count | long | number of rows in that partition |

Expected: 4 rows, sorted by `partition_id` ASC.
`repartition(4)` uses a full shuffle and distributes rows via round-robin, so the distribution
should be roughly even.\
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
## Problem 2: Partition Skew on customer_id

Repartition `orders` into 10 partitions **by the `customer_id` column** and observe how
uneven the distribution is.

**Approach:** Use `orders.repartition(10, F.col("customer_id"))`. Rows with the same
`customer_id` hash to the same partition, so `customer_id=1` (which holds ~30% of all orders)
will dominate one partition — this is data skew.

> **Note:** `customer_id=1` has approximately 30% of all 8 000 orders in this dataset.
> Partitioning by a skewed key concentrates those rows into a single partition, making
> that task far slower than the rest.

| Column | Type | Notes |
|--------|------|-------|
| partition_id | int | 0–9 |
| row_count | long | sorted DESC to highlight the hot partition |

Expected: 10 rows, sorted by `row_count` DESC.\
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
## Problem 3: Coalesce vs Repartition

Start with `orders` repartitioned to 20 partitions, then **coalesce** down to 5 and measure
the row distribution.

**Approach:** `orders.repartition(20)` first, then `.coalesce(5)`. Coalesce merges adjacent
partitions without a full shuffle — it is cheaper than `repartition` when reducing partition count.
Add `spark_partition_id()`, group, and count.

| Column | Type | Notes |
|--------|------|-------|
| partition_id | int | 0–4 |
| row_count | long | sorted ASC by partition_id |

Expected: 5 rows, sorted by `partition_id` ASC.\
""",
        ),
        (
            "code",
            """\
solution_3 = None  # ← your answer here\
""",
        ),
        ("code", "checker.p3(solution_3)"),
        (
            "markdown",
            """\
**Observation:** Run `.explain()` on `orders.repartition(20).coalesce(5)` and look at the
physical plan. You will see an `Exchange` node for the initial `repartition(20)` shuffle, but
**no second Exchange** for `coalesce(5)` — coalesce avoids the full shuffle by merging local
partitions in place.\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 4: Shuffle Partition Control

Control the number of post-shuffle partitions produced by a `groupBy` aggregation using the
`spark.sql.shuffle.partitions` config.

**Approach:**
1. Set `spark.conf.set("spark.sql.shuffle.partitions", "4")`.
2. Run `orders.groupBy("status").agg(...)`.
3. Reset the config to `"8"` afterwards so later problems are unaffected.

The output DataFrame will have at most 4 partitions because Spark uses `shuffle.partitions`
to size the exchange after the aggregate.

| Column | Type | Notes |
|--------|------|-------|
| status | string | completed / pending / cancelled / refunded |
| total_revenue | double | `round(sum(total_amount), 2)`, sorted DESC |

Expected: 4 rows, sorted by `total_revenue` DESC.\
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
## Problem 5: Range-Based Partitioning

Partition `orders` into 5 partitions using `repartitionByRange` on `total_amount`, then
summarise the value range and size of each partition.

**Approach:** `orders.repartitionByRange(5, F.col("total_amount"))` distributes rows so that
each partition covers a contiguous range of `total_amount` values — unlike hash-based
`repartition`, adjacent values end up together. Add `spark_partition_id()`, then group by it
and compute `min`, `max`, and `count`.

| Column | Type | Notes |
|--------|------|-------|
| partition_id | int | 0–4, sorted ASC |
| min_amount | double | min `total_amount` in that partition |
| max_amount | double | max `total_amount` in that partition |
| row_count | long | |

Expected: 5 rows, sorted by `partition_id` ASC.\
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

def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [

        # ── Title ────────────────────────────────────────────────────────────
        ("markdown", """\
# PySpark Gym — 05: Salting

Practice: detecting data skew, the salting technique for skewed `groupBy` and skewed joins,
and verifying that salted results match naive results.\
"""),

        # ── What is salting? ─────────────────────────────────────────────────
        ("markdown", """\
## What is Salting?

When one key value dominates a dataset, hash partitioning concentrates those rows into a
single task — one task does 30% of the work while the other 9 sit idle.

**Salting** breaks the hot key into multiple artificial sub-keys by appending a random integer
(the "salt"). The hot partition is spread across `N_SALT` tasks instead of one.

```
Original key:  "customer_1"  → all rows go to partition 3
Salted keys:   "customer_1_0", "customer_1_1", ..., "customer_1_9"
               → rows distributed across 10 partitions
```

After the partial aggregation, strip the salt suffix and do a final merge to get the
correct per-key totals.

This dataset has **intentional skew**: `customer_id=1` owns ~30% of all 8 000 orders.\
"""),

        # ── Setup ────────────────────────────────────────────────────────────
        ("code", """\
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
from functools import reduce

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

from utils.checks.salting import Checker
checker = Checker(spark, customers, products, orders, order_items)\

from utils.checks.salting import Checker
checker = Checker(spark, customers, products, orders, order_items)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 1: Identify the Skew

Before salting, confirm that the skew actually exists. Find the five customers with the most orders.

**Approach:** `groupBy("customer_id")` on `orders`, `count("order_id")` → `order_count`,
sort descending, limit 5. `customer_id=1` should appear at the top with a count far above
the rest.

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted by `order_count` DESC |
| order_count | long | |

Expected: 5 rows, ordered by `order_count` DESC.\
"""),

        ("code", """\
solution_1 = None  # ← your answer here\
"""),

        ("code", "checker.p1(solution_1)"),

        # ════════════════════════════════════════════════════════════════════
        # Problem 2
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 2: Quantify the Skew Ratio

A single number — `max_orders / avg_orders` — tells you how many times worse the hottest
partition will be versus a balanced one. Compute it.

**Approach:** Start from the per-customer counts computed in P1 (without the LIMIT).
Then aggregate again: `max(order_count)` → `max_orders`, `round(avg(order_count), 2)` →
`avg_orders`, and derive `skew_ratio = round(max_orders / avg_orders, 2)`.

| Column | Type | Notes |
|--------|------|-------|
| max_orders | long | maximum orders any single customer has |
| avg_orders | double | average orders per customer, rounded 2 dp |
| skew_ratio | double | `round(max_orders / avg_orders, 2)` |

Expected: a single-row DataFrame.\
"""),

        ("code", """\
solution_2 = None  # ← your answer here\
"""),

        ("code", "checker.p2(solution_2)"),

        # ════════════════════════════════════════════════════════════════════
        # Problem 3
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 3: Salted GroupBy — Total Revenue per Customer

Compute total revenue per customer using salting, then verify the result matches the naive approach.

**Why two passes?** The salt makes every `customer_1_*` key unique, so the first `groupBy`
is distributed across `N_SALT` tasks. The second `groupBy` strips the salt and merges partial
sums — a cheap aggregation over already-reduced data.

**Steps:**
1. Add `salt = (F.rand(seed=42) * N_SALT).cast("int")` — assigns each row a random integer 0–9.
2. Create `salted_key = concat(customer_id_str, "_", salt_str)` — the hot key is now 10 keys.
3. **Pre-aggregate:** `groupBy("salted_key")` → `sum("total_amount")` as `partial_sum`.
   This is the distributed step; no single task dominates.
4. **Strip the salt:** `F.split(col("salted_key"), "_")[0].cast("int")` recovers `customer_id`.
5. **Final aggregate:** `groupBy("customer_id")` → `round(sum("partial_sum"), 2)` as `total_revenue`.
6. Sort by `customer_id` ASC.

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| total_revenue | double | `round(sum(total_amount), 2)` |

Expected: same result as the naive `groupBy("customer_id").agg(round(sum(...)))`.
`precision=0.02` allows for minor floating-point accumulation across two-step sums.\
"""),

        ("code", """\
N_SALT = 10
solution_3 = None  # ← your answer here\
"""),

        ("code", "checker.p3(solution_3)"),

        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 4: Salted Join — Orders for Platinum Customers

The join target (`platinum` customers) is a small lookup table, but the probe side (`orders`)
is skewed: `customer_id=1` sends 30% of rows to one task. Salting distributes that work.

**Why replicate the lookup?** A normal join hashes both sides on `customer_id`. With salting,
the `orders` side uses `customer_1_0`, `customer_1_1`, …, `customer_1_4` as keys — the lookup
must have a matching row for *each* salt value, otherwise those orders would be dropped.
Replicating the lookup `N_SALT` times with every salt value guarantees every salted orders
row finds a match.

**Steps:**
1. `lookup = customers.filter(tier == "platinum").select("customer_id", "name")`
2. **Replicate the lookup:** for each salt `i` in `range(N_SALT)`:
   - add `salt_col = lit(i)`
   - add `salted_key = concat(customer_id_str, "_", lit(str(i)))`
   - `union` all copies together.
3. **Salt the orders side:** add `(F.rand(seed=42) * N_SALT).cast("int")` as `salt`,
   create matching `salted_key = concat(customer_id_str, "_", salt_str)`.
4. **Join** on `salted_key`.
5. **Select:** `order_id`, `customer_id` (from orders), `name`, `total_amount`. Sort `order_id` ASC.

| Column | Type | Notes |
|--------|------|-------|
| order_id | int | sorted ASC |
| customer_id | int | |
| name | string | customer name |
| total_amount | double | |

Expected: same rows as a direct `orders.join(lookup, "customer_id")`.\
"""),

        ("code", """\
N_SALT = 5
solution_4 = None  # ← your answer here\
"""),

        ("code", "checker.p4(solution_4)"),

        # ════════════════════════════════════════════════════════════════════
        # Problem 5
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 5: Compare Partition Hotspot Before and After Salting

Put a number on the improvement. Build both the un-salted and salted repartitioned DataFrames
and compare their worst-case partition imbalance.

**Approach:**
- **Unsalted:** `orders.repartition(10, col("customer_id"))` — hash partition on the raw key.
- **Salted:** add a random salt column, create `salted_key = concat(customer_id_str, "_", salt_str)`,
  then `repartition(10, col("salted_key"))`.

For each, use `spark_partition_id()` + `groupBy` + `count` to get rows per partition, then
reduce to three stats:
- `max_partition_rows` — the hottest partition
- `avg_partition_rows` — baseline (cast to long after `round(avg, 0)`)
- `hottest_ratio` — `round(max / avg, 2)` — the skew multiplier

Return a two-row DataFrame with an `approach` column (`"unsalted"` / `"salted"`).

| Column | Type | Notes |
|--------|------|-------|
| approach | string | "salted" or "unsalted", sorted ASC |
| max_partition_rows | long | |
| avg_partition_rows | long | `round(avg, 0).cast("long")` |
| hottest_ratio | double | `round(max / avg, 2)` — lower is better |

A well-salted result should have a `hottest_ratio` close to 1.0; unsalted will be much higher.\
"""),

        ("code", """\
solution_5 = None  # ← your answer here\
"""),

        ("code", "checker.p5(solution_5)"),

    ]

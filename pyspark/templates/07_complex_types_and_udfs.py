def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# PySpark Gym — 07: Complex Types, Higher-Order Functions & UDFs

Practice: building array/struct columns, transforming them with native higher-order
functions (`transform`, `filter`, `aggregate`, `exists`, `forall`), arg-max via struct
ordering, and the performance gap between row-at-a-time Python UDFs and vectorized
Pandas UDFs.\
""",
        ),
        # ── Why this matters ─────────────────────────────────────────────────
        (
            "markdown",
            """\
## Why this matters

Reaching for a Python UDF is often the *slowest* correct solution: each row crosses the
JVM↔Python boundary one at a time, and Catalyst can't optimize through it. Native
higher-order array functions and struct tricks run entirely inside the JVM and stay
optimizable. A **Pandas UDF** (`@F.pandas_udf`) splits the gap — it still leaves the
JVM, but processes a whole Arrow-backed batch per call instead of one row at a time.

Senior-level rule of thumb: prefer native functions → Pandas UDF → Python UDF, in that
order, and only drop a level when the native API genuinely can't express the logic.\
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
from pyspark.sql.types import StringType
import pandas as pd

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
print(f"order_items: {order_items.count():>6,}")

from utils.checks.complex_types_udfs import Checker
checker = Checker(spark, customers, products, orders, order_items)\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 1: Build a Per-Customer Order Amount Array

For each customer, collect all their `total_amount` values into a single sorted array
column.

<details>
<summary>Hint</summary>

`orders.groupBy("customer_id").agg(F.array_sort(F.collect_list("total_amount")).alias("amounts"))`.
`collect_list` has no guaranteed order, so wrap it in `array_sort` to make the result
deterministic. Sort the final DataFrame by `customer_id` ASC.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| amounts | array<double> | ascending within each array |

Expected: 500 rows, one array per customer.\
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
## Problem 2: Higher-Order Functions — filter, aggregate, size

Starting from the per-customer `amounts` array (Problem 1), compute three derived
columns using only native array functions — no UDF, no `explode`.

<details>
<summary>Hint</summary>

- `order_count`: `F.size("amounts")`
- `large_count`: `F.size(F.filter("amounts", lambda x: x > 100))` — counts elements
  satisfying a predicate without exploding the array.
- `total_amount_sum`: `F.round(F.aggregate("amounts", F.lit(0.0), lambda acc, x: acc + x), 2)`
  — `aggregate` is a fold: starting accumulator, then a combine lambda applied
  element-by-element.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| order_count | int | total elements in the array |
| large_count | int | elements > 100 |
| total_amount_sum | double | `round(sum(amounts), 2)` |

Expected: 500 rows.\
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
## Problem 3: exists() and forall() — Per-Customer Flags

Flag each customer with two booleans derived from their `amounts` array: whether *any*
order was large, and whether *every* order was above a baseline.

<details>
<summary>Hint</summary>

`F.exists("amounts", lambda x: x > 4000)` → true if at least one element matches.
`F.forall("amounts", lambda x: x > 500)` → true only if *all* elements match. Both
short-circuit internally and never leave the JVM.

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| has_large_order | boolean | any order > 4000 |
| all_above_500 | boolean | every order > 500 |

Expected: 500 rows; roughly half will have `has_large_order = true`.\
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
## Problem 4: Struct Ordering — Biggest Order per Customer

Find each customer's single largest order (by `total_amount`) using struct comparison
instead of a window function.

<details>
<summary>Hint</summary>

Structs compare field-by-field in declaration order, so
`F.struct("total_amount", "order_id")` sorts primarily by amount. Collecting these into
an array and taking `F.array_max` gives the "argmax" row directly — no window, no
explode, no self-join:

```python
orders.groupBy("customer_id").agg(
    F.array_max(F.collect_list(F.struct("total_amount", "order_id"))).alias("biggest")
).select(
    "customer_id",
    F.col("biggest.order_id").alias("biggest_order_id"),
    F.col("biggest.total_amount").alias("biggest_amount"),
)
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | sorted ASC |
| biggest_order_id | int | order_id of the largest order |
| biggest_amount | double | that order's total_amount |

Expected: 500 rows — same result you'd get from a `ROW_NUMBER()` window, computed
without a shuffle-heavy window function.\
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
## Problem 5: Python UDF vs Pandas UDF — Loyalty Tier

Classify each order into a loyalty tier (`gold` ≥ 4000, `silver` ≥ 1000, else `bronze`)
two ways — a plain Python UDF and a vectorized Pandas UDF — and confirm they agree.
Submit the **Python UDF** version as your solution.

<details>
<summary>Hint</summary>

```python
def tier_label(amount):
    if amount >= 4000: return "gold"
    elif amount >= 1000: return "silver"
    else: return "bronze"

python_udf = F.udf(tier_label, StringType())

@F.pandas_udf(StringType())
def tier_label_vectorized(amounts: pd.Series) -> pd.Series:
    return amounts.apply(tier_label)
```

Apply `python_udf` to `total_amount` for your submitted `solution_5`. Then, on your own,
apply `tier_label_vectorized` to the same column and confirm
`.exceptAll(...)` between the two results is empty — the *output* is identical, only the
execution strategy differs. The Pandas UDF amortizes the Python round-trip over a whole
Arrow batch instead of one row at a time, which matters once you're doing this over
millions of rows rather than 8 000.

</details>

| Column | Type | Notes |
|--------|------|-------|
| order_id | int | sorted ASC |
| customer_id | int | |
| total_amount | double | |
| loyalty_tier | string | "gold" / "silver" / "bronze" |

Expected: 8 000 rows.\
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

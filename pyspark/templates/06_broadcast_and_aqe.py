def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# PySpark Gym — 06: Broadcast Joins & Adaptive Query Execution

Practice: explicit broadcast joins, reading physical plans to confirm join strategy,
overriding the optimizer with join hints, and Adaptive Query Execution (AQE) partition
coalescing. These are the tools senior engineers reach for when a join or aggregation
is slow and `EXPLAIN` is the first thing to check.\
""",
        ),
        # ── What is AQE? ─────────────────────────────────────────────────────
        (
            "markdown",
            """\
## Broadcast Joins and AQE, briefly

A **broadcast join** ships a small table to every executor so the large side never
shuffles — the join becomes a local hash lookup per partition. Spark does this
automatically when a table is smaller than `spark.sql.autoBroadcastJoinThreshold`
(default 10MB), but you can force it with `F.broadcast(df)` or suppress it with
`.hint("merge")` / `.hint("shuffle_hash")`.

**Adaptive Query Execution (AQE)** re-optimizes the plan at runtime using actual
shuffle statistics instead of static estimates. Two AQE features matter most day to day:
- **Coalescing post-shuffle partitions** — merges tiny partitions so a `groupBy` over a
  small dataset doesn't leave 200 nearly-empty tasks.
- **Skew join handling** — splits an oversized partition into smaller pieces so one hot
  key doesn't stall the whole stage.

Use `df.explain(mode="formatted")` to read the physical plan and confirm what actually
ran — `BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin` all mean different things
for cost and memory.\
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
import io
from contextlib import redirect_stdout

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

def explain_str(df) -> str:
    \"\"\"Capture df.explain(mode='formatted') as a string instead of printing it.\"\"\"
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()

from utils.checks.broadcast_aqe import Checker
checker = Checker(spark, customers, products, orders, order_items)\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 1: Broadcast Join — Order Revenue with Customer Tier

`customers` is tiny (500 rows) relative to `orders` (8 000 rows). Join them using an
**explicit broadcast hint** so the join never shuffles the larger side.

<details>
<summary>Hint</summary>

`orders.join(F.broadcast(customers), "customer_id")`, then select
`order_id`, `customer_id`, `tier`, `total_amount`, sorted by `order_id` ASC.

</details>

| Column | Type | Notes |
|--------|------|-------|
| order_id | int | sorted ASC |
| customer_id | int | |
| tier | string | |
| total_amount | double | |

Expected: 8 000 rows.\
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
## Problem 2: Read the Physical Plan — Disabled Threshold

Disable auto-broadcast entirely, then compare a plain join against an explicitly
broadcast join. Confirm the join strategy by inspecting the physical plan text, not by
guessing.

<details>
<summary>Hint</summary>

1. `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)` — disables automatic
   broadcasting, even for tiny tables.
2. Build `no_hint = orders.join(customers, "customer_id")` and
   `explicit = orders.join(F.broadcast(customers), "customer_id")`.
3. Use `explain_str(df)` (defined in setup) and check whether `"BroadcastHashJoin"`
   appears in the output for each.
4. Return a two-row DataFrame, then restore the threshold:
   `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| join_type | string | "explicit_broadcast" or "no_hint", sorted ASC |
| uses_broadcast | boolean | whether the plan contains `BroadcastHashJoin` |

Expected: `explicit_broadcast` → `True`, `no_hint` → `False`. The hint overrides the
disabled threshold; without it, Spark falls back to a shuffle-based join.\
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
## Problem 3: Force a Sort-Merge Join with `.hint("merge")`

`customers` is small enough to auto-broadcast by default. Force Spark to use a
sort-merge join instead via a join hint, and confirm it in the plan.

<details>
<summary>Hint</summary>

With the default threshold restored, `orders.join(customers, "customer_id")` will use
`BroadcastHashJoin`. Add `.hint("merge")` to the right-hand side —
`orders.join(customers.hint("merge"), "customer_id")` — and Spark obeys the hint even
though it would normally broadcast. Check both plans with `explain_str` for
`"BroadcastHashJoin"` and `"SortMergeJoin"`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| join_type | string | "default" or "merge_hint", sorted ASC |
| uses_broadcast | boolean | |
| uses_sort_merge | boolean | |

Expected: `default` → broadcast=True, sort_merge=False. `merge_hint` → broadcast=False,
sort_merge=True.\
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
## Problem 4: Multi-Way Broadcast Join — Revenue by Category and Tier

Join `order_items` to `products` and `orders` to `customers`, broadcasting both small
lookup tables, and compute revenue per product category and customer tier.

<details>
<summary>Hint</summary>

Broadcast `products` and `customers` (both small); `order_items` and `orders` stay on
the probe side since they're the larger tables:

```python
joined = (
    order_items
    .join(F.broadcast(products), "product_id")
    .join(orders, "order_id")
    .join(F.broadcast(customers), "customer_id")
)
```

Then `groupBy("category", "tier")`, aggregate
`round(sum(quantity * unit_price), 2)` as `total_revenue` and `count("item_id")` as
`item_count`, sorted by `category`, `tier` ASC.

</details>

| Column | Type | Notes |
|--------|------|-------|
| category | string | sorted ASC |
| tier | string | sorted ASC |
| total_revenue | double | `round(sum(quantity * unit_price), 2)` |
| item_count | long | |

Expected: one row per (category, tier) combination present in the data.\
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
## Problem 5: AQE Coalesce Partitions — Shrinking Tiny Shuffle Output

Set `spark.sql.shuffle.partitions` artificially high (200) for a `groupBy` over
`order_items` (only 100 distinct products), then compare the resulting partition count
with AQE's coalescing disabled vs enabled.

<details>
<summary>Hint</summary>

1. `spark.conf.set("spark.sql.shuffle.partitions", "200")`.
2. With `spark.conf.set("spark.sql.adaptive.enabled", "false")`, run
   `order_items.groupBy("product_id").agg(F.sum("quantity").alias("q"))`, call
   `.count()` to force execution, then read `.rdd.getNumPartitions()`.
3. Flip `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled`
   to `"true"`, rebuild the same aggregation, and read its partition count.
4. Return a two-row DataFrame, then reset
   `spark.conf.set("spark.sql.shuffle.partitions", "8")` for later problems.

</details>

| Column | Type | Notes |
|--------|------|-------|
| scenario | string | "aqe_coalesced" or "no_aqe", sorted ASC |
| num_partitions | int | |

Expected: `no_aqe` stays at the requested 200 partitions; `aqe_coalesced` collapses to a
handful (or one) because the actual shuffle output is tiny — AQE measured the real data
size and merged the empty partitions away.\
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

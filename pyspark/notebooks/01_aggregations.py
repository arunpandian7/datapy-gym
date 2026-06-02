def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [

        # ── Title ────────────────────────────────────────────────────────────
        ("markdown", """\
# PySpark Gym — 01: Aggregations

Practice: `groupBy`, `agg`, conditional counts, date bucketing, and multi-column aggregation.
Each problem builds a result DataFrame; assign it to the named `solution_N` variable and run the check cell.\
"""),

        # ── Setup ────────────────────────────────────────────────────────────
        ("code", """\
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
## Problem 1: Revenue by Product Category

For every product category, compute the total revenue generated across all order line items.

**Approach hint:** Join `order_items` with `products` on `product_id`, then group by `category`
and sum `quantity * unit_price`.

| Column | Type | Notes |
|--------|------|-------|
| category | string | product category |
| total_revenue | double | `round(sum(quantity * unit_price), 2)`, sorted DESC |

Expected: **10 rows** (one per category).\
"""),

        ("code", """\
solution_1 = None  # ← your answer here\
"""),

        ("code", """\
_joined_1 = order_items.join(products, "product_id")
_expected_1 = (
    _joined_1
    .groupBy("category")
    .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_revenue"))
    .orderBy(F.col("total_revenue").desc())
)
check(solution_1, _expected_1, problem="P1: Revenue by Product Category")\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 2
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 2: Top 5 Customers by Completed Spend

Find the five customers who spent the most on **completed** orders.

**Approach hint:** Filter `orders` to `status == "completed"`, join with `customers` on
`customer_id`, group by `customer_id` + `name`, sum `total_amount`.

| Column | Type | Notes |
|--------|------|-------|
| customer_id | int | |
| name | string | customer name |
| total_spend | double | `round(sum(total_amount), 2)`, sorted DESC |

Expected: **5 rows**, ordered by `total_spend` DESC.\
"""),

        ("code", """\
solution_2 = None  # ← your answer here\
"""),

        ("code", """\
_completed_2 = orders.filter(F.col("status") == "completed")
_expected_2 = (
    _completed_2
    .join(customers, "customer_id")
    .groupBy("customer_id", "name")
    .agg(F.round(F.sum("total_amount"), 2).alias("total_spend"))
    .orderBy(F.col("total_spend").desc())
    .limit(5)
)
check(solution_2, _expected_2, problem="P2: Top 5 Customers by Completed Spend", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 3
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 3: Monthly Revenue Trend

Aggregate orders by calendar month and track order volume alongside revenue.

**Approach hint:** Use `F.date_format(order_date, "yyyy-MM")` to extract the month string,
then group and aggregate.

| Column | Type | Notes |
|--------|------|-------|
| month | string | `"yyyy-MM"` format |
| order_count | long | number of orders in the month |
| monthly_revenue | double | `round(sum(total_amount), 2)` |

Expected: one row per month, ordered by `month` ASC.\
"""),

        ("code", """\
solution_3 = None  # ← your answer here\
"""),

        ("code", """\
_expected_3 = (
    orders
    .withColumn("month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("month")
    .agg(
        F.count("order_id").alias("order_count"),
        F.round(F.sum("total_amount"), 2).alias("monthly_revenue"),
    )
    .orderBy("month")
)
check(solution_3, _expected_3, problem="P3: Monthly Revenue Trend", ordered=True)\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 4: Categories Where Average Item Price Exceeds $100

Identify product categories where the average unit price of items sold is above $100.

**Approach hint:** Join `order_items` with `products` on `product_id`, group by `category`,
compute `avg(unit_price)`, then filter.

| Column | Type | Notes |
|--------|------|-------|
| category | string | |
| avg_item_price | double | `round(avg(unit_price), 2)`, filtered > 100, sorted DESC |

Expected: subset of the 10 categories.\
"""),

        ("code", """\
solution_4 = None  # ← your answer here\
"""),

        ("code", """\
_joined_4 = order_items.join(products, "product_id")
_expected_4 = (
    _joined_4
    .groupBy("category")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_item_price"))
    .filter(F.col("avg_item_price") > 100)
    .orderBy(F.col("avg_item_price").desc())
)
check(solution_4, _expected_4, problem="P4: Categories Where Average Item Price Exceeds $100")\
"""),

        # ════════════════════════════════════════════════════════════════════
        # Problem 5
        # ════════════════════════════════════════════════════════════════════
        ("markdown", """\
## Problem 5: Customer Tier Performance Summary

Summarise order activity and revenue by customer tier, then compute revenue per customer.

**Approach hint:** Join `orders` with `customers` on `customer_id`, group by `tier`, and use
`countDistinct` for unique customers. Derive `revenue_per_customer` as a column expression
after aggregation.

| Column | Type | Notes |
|--------|------|-------|
| tier | string | bronze / silver / gold / platinum |
| unique_customers | long | `countDistinct(customer_id)` |
| total_orders | long | `count(order_id)` |
| total_revenue | double | `round(sum(total_amount), 2)` |
| revenue_per_customer | double | `round(total_revenue / unique_customers, 2)` |

Expected: one row per tier, ordered by `tier` ASC.\
"""),

        ("code", """\
solution_5 = None  # ← your answer here\
"""),

        ("code", """\
_expected_5 = (
    orders
    .join(customers, "customer_id")
    .groupBy("tier")
    .agg(
        F.countDistinct("customer_id").alias("unique_customers"),
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )
    .withColumn("revenue_per_customer", F.round(F.col("total_revenue") / F.col("unique_customers"), 2))
    .orderBy("tier")
)
check(solution_5, _expected_5, problem="P5: Customer Tier Performance Summary")\
"""),

    ]

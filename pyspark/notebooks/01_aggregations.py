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

from utils.checks.aggregations import Checker
checker = Checker(spark, customers, products, orders, order_items)\

from utils.checks.aggregations import Checker
checker = Checker(spark, customers, products, orders, order_items)\
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

        ("code", "checker.p1(solution_1)"),

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

        ("code", "checker.p2(solution_2)"),

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

        ("code", "checker.p3(solution_3)"),

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

        ("code", "checker.p4(solution_4)"),

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

        ("code", "checker.p5(solution_5)"),

    ]

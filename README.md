# datapy-gym

## What this is

A local data engineering interview prep gym with two practice tracks:

**PySpark track** — five notebooks covering the topics that come up most in data engineering coding rounds: aggregations, window functions, joins, partitioning, and salting. Synthetic e-commerce dataset (500 customers, 100 products, 8 000 orders, ~20 000 line items) with intentional skew baked in.

**SQL/DuckDB track** — five notebooks covering SQL interview topics using a fintech/payments dataset (users, merchants, accounts, 20 000 transactions). DuckDB runs embedded — no server needed. Topics: aggregations, window functions, joins, CTEs & recursive queries, and time series analysis. SQL dialect is close to Snowflake and PostgreSQL; hints call out cross-warehouse portability.

Each problem has a check cell that validates your answer automatically. Sessions are date-stamped directories so your git history is a record of every attempt.

---

## Prerequisites

- Python 3.13+
- Java 11+ (required by PySpark — check with `java -version`)
- uv (`pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Nothing else — all dependencies are installed in the project venv by `uv sync`

---

## Setup (one-time)

```bash
git clone <repo-url>
cd datapy-gym
uv sync                                        # creates .venv with all deps

# PySpark track
uv run python pyspark/data/generate_data.py   # generates e-commerce CSVs
uv run python pyspark/reset.py                # generates today's notebooks

# SQL track
uv run python sql/data/generate_data.py       # generates fintech CSVs
uv run python sql/reset.py                    # generates today's notebooks
```

---

## Opening notebooks

### JupyterLab

```bash
# PySpark track
uv run jupyter lab pyspark/sessions/$(date +%F)/

# SQL track
uv run jupyter lab sql/sessions/$(date +%F)/
```

### VS Code

1. Install the [Jupyter extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) (`ms-toolsai.jupyter`)
2. Open any `.ipynb` file directly from the `sessions/` directory
3. Select the kernel: click **Select Kernel** (top-right) → **Python Environments** → choose `.venv`
   - If `.venv` isn't listed: `Ctrl+Shift+P` → **Python: Select Interpreter** → pick `./.venv/bin/python`

---

## Daily workflow

```bash
# PySpark
uv run python pyspark/reset.py
uv run jupyter lab pyspark/sessions/$(date +%F)/    # or open in VS Code

# SQL
uv run python sql/reset.py
uv run jupyter lab sql/sessions/$(date +%F)/        # or open in VS Code

# commit your work after each session
git add pyspark/sessions/ sql/sessions/
git commit -m "session: $(date +%F)"
```

---

## Resetting notebooks

```bash
# PySpark
uv run python pyspark/reset.py              # all 5 notebooks, prompts before overwrite
uv run python pyspark/reset.py 03           # just notebook 03 (joins)
uv run python pyspark/reset.py --force      # skip overwrite prompt

# SQL
uv run python sql/reset.py                  # all 5 notebooks
uv run python sql/reset.py 04               # just notebook 04 (CTEs)
uv run python sql/reset.py --force
```

---

## Topics

### PySpark track (e-commerce dataset)

| # | Notebook | What it covers |
|---|----------|----------------|
| 01 | Aggregations | `groupBy`, `agg`, HAVING-like filters, date bucketing, `countDistinct` |
| 02 | Window Functions | Ranking (`rank`/`dense_rank`/`row_number`), cumulative sums, `lag`/`lead`, `percent_rank` |
| 03 | Joins | Inner/left/anti/semi joins, self-join, broadcast hint, multi-table joins |
| 04 | Partitioning | `repartition` vs `coalesce`, partition-by-column, `repartitionByRange`, shuffle partition control |
| 05 | Salting | Skew detection, salted `groupBy` (two-pass), salted join (replicated lookup), hotspot comparison |

### SQL track (fintech/payments dataset, DuckDB)

| # | Notebook | What it covers |
|---|----------|----------------|
| 01 | Aggregations | `GROUP BY`, `HAVING`, `DATE_TRUNC`, conditional agg (`FILTER`/`CASE WHEN`), multi-table `LEFT JOIN` |
| 02 | Window Functions | Running totals, `DENSE_RANK`+`QUALIFY`, `LAG`, `ROW_NUMBER`, `PERCENT_RANK` |
| 03 | Joins | 4-table `INNER JOIN`, anti-join, self-join, subquery intersection, derived-table comparison |
| 04 | CTEs | Multi-step CTE chains, `QUALIFY`, deduplication, recursive CTE (date series) |
| 05 | Time Series | Rolling windows, cohort analysis, gap detection, year-over-year, anomaly detection |

---

## How problems work

### PySpark

Each notebook follows the same pattern: markdown description → solution stub (assign a DataFrame) → check cell.

```python
solution_1 = None  # ← your answer here
```

```python
checker.p1(solution_1)
```

### SQL

Same pattern, but the solution is a SQL string:

```python
solution_1 = """
SELECT category, ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM order_items JOIN products USING (product_id)
GROUP BY category
ORDER BY total_revenue DESC
"""
checker.p1(solution_1)
```

Running the check cell renders inline feedback:

```
P1: Total Spending by MCC Category
✓ columns match: ['mcc_category', 'total_spent']
✓ row count: 8
✓ values match
All checks passed!
```

---

## Datasets

### PySpark — e-commerce

| Table | Rows | Key columns |
|-------|------|-------------|
| `customers` | 500 | `customer_id`, `name`, `city`, `country`, `tier`, `signup_date` |
| `products` | 100 | `product_id`, `name`, `category`, `brand`, `price` |
| `orders` | 8 000 | `order_id`, `customer_id`, `order_date`, `status`, `total_amount` |
| `order_items` | ~20 000 | `item_id`, `order_id`, `product_id`, `quantity`, `unit_price` |

`customer_id=1` owns 30% of all orders — deliberate skew for the salting notebook.

### SQL — fintech/payments

| Table | Rows | Key columns |
|-------|------|-------------|
| `users` | 500 | `user_id`, `name`, `country`, `tier` (basic/premium/business), `created_date` |
| `merchants` | 200 | `merchant_id`, `name`, `mcc_category` (8 categories), `city`, `country` |
| `accounts` | 600 | `account_id`, `user_id`, `account_type` (checking/savings/credit), `status` |
| `transactions` | 20 000 | `txn_id`, `account_id`, `merchant_id`, `amount`, `txn_type`, `txn_date`, `status` |

Top 20 accounts hold 30% of transactions — skew for realistic window function and aggregation problems.

---

## Adding problems

Edit the relevant `*/templates/NN_topic.py` and add a new `(markdown, ...)` / `(code, ...)` / `(code, ...)` triple following the existing pattern. Regenerate with `uv run python */reset.py NN`.

The `templates/` directory is the source of truth — `.py` files there define every problem. The `.ipynb` notebooks in `sessions/` are generated from them and should never be edited directly.

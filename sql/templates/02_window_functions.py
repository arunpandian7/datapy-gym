def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 02: Window Functions

Practice: `OVER`, `PARTITION BY`, frame specifications, ranking functions, `LAG`/`LEAD`, and running aggregates.
Assign your SQL string to the named `solution_N` variable and run the check cell.

**Tables:** `users`, `accounts`, `merchants`, `transactions`\
""",
        ),
        # ── Setup ────────────────────────────────────────────────────────────
        (
            "code",
            """\
from pathlib import Path
import sys

_cwd = Path.cwd()
_candidates = [_cwd / "sql", _cwd, _cwd.parent, _cwd.parent / "sql",
               _cwd.parent.parent, _cwd.parent.parent / "sql"]
_sql_dir = next((p for p in _candidates if (p / "utils" / "__init__.py").exists()), None)
if _sql_dir is None:
    raise RuntimeError(
        "Cannot locate sql/utils. Run: uv run jupyter lab from the project root."
    )
if str(_sql_dir) not in sys.path:
    sys.path.insert(0, str(_sql_dir))

DATA_DIR = _sql_dir / "data"

from utils import get_conn, check
from utils.checks.window_functions import Checker

conn = get_conn(DATA_DIR)
checker = Checker(conn)
print("Ready. Tables: users, merchants, accounts, transactions")\
""",
        ),
        # ── Data preview ─────────────────────────────────────────────────────
        (
            "code",
            """\
for table in ["users", "merchants", "accounts", "transactions"]:
    print(f"\\n{'─'*50}\\n  {table}\\n{'─'*50}")
    display(conn.execute(f"SELECT * FROM {table} LIMIT 3").df())
    n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  ({n:,} rows total)")\
""",
        ),
        # ════════════════════════════════════════════════════════════════════
        # Problem 1
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 1: Running Balance per Account

For each completed transaction, compute the running balance of the account. Treat debits as negative and credits as positive. Order by account_id, txn_date, txn_id within each partition.

<details>
<summary>Hint</summary>

Use `SUM(CASE WHEN txn_type = 'credit' THEN amount ELSE -amount END) OVER (PARTITION BY account_id ORDER BY txn_date, txn_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. The `ROWS BETWEEN` frame spec makes this a true running (not sliding) total. Filter `status = 'completed'` before windowing.

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_id | integer | |
| account_id | integer | |
| txn_date | date | |
| amount | double | |
| txn_type | string | |
| running_balance | double | `ROUND(..., 2)`, cumulative signed sum |

Expected: all completed transactions ordered by account_id, txn_date, txn_id.\
""",
        ),
        (
            "code",
            """\
solution_1 = None  # ← your SQL here\
""",
        ),
        ("code", "checker.p1(solution_1)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 2
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 2: Top 3 Merchants by Revenue per Category

For each MCC category, rank merchants by their total completed revenue and return the top 3. Use `DENSE_RANK` so ties are handled correctly.

<details>
<summary>Hint</summary>

First aggregate revenue per merchant in a CTE. Then apply `DENSE_RANK() OVER (PARTITION BY mcc_category ORDER BY total_revenue DESC)`. Use `QUALIFY revenue_rank <= 3` to filter in DuckDB/Snowflake. In PostgreSQL/Redshift, wrap in a subquery or CTE and filter there. Note: `QUALIFY` is shorthand — both approaches are valid in interviews.

</details>

| Column | Type | Notes |
|--------|------|-------|
| merchant_id | integer | |
| name | string | |
| mcc_category | string | |
| total_revenue | double | `ROUND(SUM, 2)` |
| revenue_rank | bigint | `DENSE_RANK` per category |

Expected: up to 3 rows per category (8 categories = ≤24 rows total), ordered by mcc_category ASC, revenue_rank ASC.\
""",
        ),
        (
            "code",
            """\
solution_2 = None  # ← your SQL here\
""",
        ),
        ("code", "checker.p2(solution_2)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 3
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 3: Month-over-Month Spend Change by Country

For each country, compute monthly total debit spending and the month-over-month percentage change. Null is expected for the first month of each country.

<details>
<summary>Hint</summary>

Use a CTE to compute monthly spend per country. In the outer query, use `LAG(monthly_spend, 1) OVER (PARTITION BY country ORDER BY month)` to access the previous month. Percentage change: `(current - previous) / previous * 100`. Handle nulls — the first row per partition will have `NULL` prev value.

</details>

| Column | Type | Notes |
|--------|------|-------|
| country | string | |
| month | date | `DATE_TRUNC` result |
| monthly_spend | double | `ROUND(..., 2)` |
| prev_month_spend | double | `ROUND(..., 2)`, NULL for first month |
| mom_pct_change | double | `ROUND(..., 2)`, NULL for first month |

Expected: rows ordered by country ASC, month ASC.\
""",
        ),
        (
            "code",
            """\
solution_3 = None  # ← your SQL here\
""",
        ),
        ("code", "checker.p3(solution_3)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 4
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 4: Top 2 Users by Spend per Country

Within each country, find the top 2 users by total completed debit spending. Use `ROW_NUMBER` so each country has exactly 2 rows (no ties in ranking).

<details>
<summary>Hint</summary>

Use a CTE to aggregate spend per user. Apply `ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_spend DESC)`. Then `QUALIFY spend_rank <= 2`. The difference from DENSE_RANK: ROW_NUMBER gives unique ranks even for ties, ensuring exactly 2 per country.

</details>

| Column | Type | Notes |
|--------|------|-------|
| user_id | integer | |
| name | string | |
| country | string | |
| tier | string | |
| total_spend | double | `ROUND(..., 2)` |
| spend_rank | bigint | `ROW_NUMBER` per country |

Expected: **10 rows** (2 per country × 5 countries), ordered by country ASC, spend_rank ASC.\
""",
        ),
        (
            "code",
            """\
solution_4 = None  # ← your SQL here\
""",
        ),
        ("code", "checker.p4(solution_4)"),
        # ════════════════════════════════════════════════════════════════════
        # Problem 5
        # ════════════════════════════════════════════════════════════════════
        (
            "markdown",
            """\
## Problem 5: Percentile Rank of Transaction Amounts

For each completed transaction, compute its percentile rank within its MCC category. A value of 0.0 is the lowest; 1.0 is the highest.

<details>
<summary>Hint</summary>

`PERCENT_RANK() OVER (PARTITION BY mcc_category ORDER BY amount)` — returns a float in [0.0, 1.0]. No CTE needed. Filter `status = 'completed'`. This is identical in DuckDB, Snowflake, BigQuery, and PostgreSQL.

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_id | integer | |
| mcc_category | string | |
| amount | double | |
| pct_rank | double | `ROUND(PERCENT_RANK(), 4)` |

Expected: all completed transactions, ordered by mcc_category ASC, amount ASC.\
""",
        ),
        (
            "code",
            """\
solution_5 = None  # ← your SQL here\
""",
        ),
        ("code", "checker.p5(solution_5)"),
    ]

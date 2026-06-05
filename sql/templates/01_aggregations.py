def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 01: Aggregations

Practice: `GROUP BY`, `HAVING`, conditional aggregation, date bucketing, and multi-table aggregation.
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

from utils import get_conn, check, register_sql_magic
from utils.checks.aggregations import Checker

conn = get_conn(DATA_DIR)
checker = Checker(conn)
register_sql_magic()
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
## Problem 1: Total Spending by MCC Category

Compute total debit spending across all completed transactions for each merchant category.

<details>
<summary>Hint</summary>

Join `transactions` with `merchants` on `merchant_id`. Filter `txn_type = 'debit'` and `status = 'completed'`. Group by `mcc_category` and sum `amount`. Sort descending.

</details>

| Column | Type | Notes |
|--------|------|-------|
| mcc_category | string | merchant category |
| total_spent | double | `ROUND(SUM(amount), 2)`, sorted DESC |

Expected: **8 rows** (one per MCC category).\
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
## Problem 2: Top 10 Merchants by Transaction Count

Find the 10 most frequently used merchants across all transactions (any status), showing their name, category, transaction count, and total transaction amount.

<details>
<summary>Hint</summary>

Join `transactions` with `merchants`. No status filter needed (count all). Group by `merchant_id`, `name`, `mcc_category`. ORDER BY `txn_count DESC`, LIMIT 10.

</details>

| Column | Type | Notes |
|--------|------|-------|
| merchant_id | integer | |
| name | string | merchant name |
| mcc_category | string | |
| txn_count | bigint | `COUNT(*)` |
| total_amount | double | `ROUND(SUM(amount), 2)` |

Expected: **10 rows**.\
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
## Problem 3: Monthly Transactions by Account Type

For completed transactions, show the monthly transaction count and total amount broken down by account type. Use `DATE_TRUNC` for the monthly bucket.

<details>
<summary>Hint</summary>

Join `transactions` with `accounts` on `account_id`. Filter `status = 'completed'`. Group by `DATE_TRUNC('month', txn_date)::DATE` and `account_type`. In DuckDB and PostgreSQL: `DATE_TRUNC('month', col)`. Snowflake uses the same syntax. BigQuery reverses args: `DATE_TRUNC(col, MONTH)`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| month | date | `DATE_TRUNC('month', txn_date)::DATE` |
| account_type | string | checking / savings / credit |
| txn_count | bigint | `COUNT(*)` |
| total_amount | double | `ROUND(SUM(amount), 2)` |

Expected: rows ordered by month ASC, account_type ASC.\
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
## Problem 4: High-Value Users (Avg Debit > $500)

Find users whose average completed debit transaction amount exceeds $500. Return their user info, average transaction amount, and how many qualifying transactions they have.

<details>
<summary>Hint</summary>

Three-table join: `transactions → accounts → users`. Filter `txn_type = 'debit'` and `status = 'completed'`. Use `HAVING AVG(amount) > 500`. Note: HAVING filters *after* grouping; WHERE filters before.

</details>

| Column | Type | Notes |
|--------|------|-------|
| user_id | integer | |
| name | string | |
| country | string | |
| tier | string | |
| avg_txn_amount | double | `ROUND(AVG(amount), 2)`, sorted DESC |
| txn_count | bigint | |

Expected: variable number of rows, sorted by avg_txn_amount DESC.\
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
## Problem 5: Country-Level Summary

Build a country-level summary showing the number of distinct users, accounts, transactions, and total completed transaction amount per country.

<details>
<summary>Hint</summary>

Start from `users`, LEFT JOIN `accounts`, LEFT JOIN `transactions`. Use `COUNT(DISTINCT ...)` for users and accounts. Use `CASE WHEN status = 'completed' THEN amount ELSE 0 END` inside SUM for the amount. Sort by `user_count DESC`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| country | string | |
| user_count | bigint | `COUNT(DISTINCT user_id)` |
| account_count | bigint | `COUNT(DISTINCT account_id)` |
| txn_count | bigint | `COUNT(DISTINCT txn_id)` |
| total_completed_amount | double | `ROUND(SUM(CASE WHEN status='completed' THEN amount ELSE 0 END), 2)` |

Expected: **5 rows** (one per country: US, UK, IN, SG, AU).\
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

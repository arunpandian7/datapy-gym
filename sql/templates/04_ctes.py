def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 04: CTEs & Recursive Queries

Practice: multi-step `WITH` chains, `QUALIFY`, deduplication patterns, and recursive CTEs for date series generation.
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
from utils.checks.ctes import Checker

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
## Problem 1: Above-Average Merchants (Multi-Step CTE)

Find merchants whose total completed revenue exceeds their MCC category's average. Show each merchant's revenue, transaction count, the category average, and the ratio of merchant revenue to category average.

<details>
<summary>Hint</summary>

Three CTEs: (1) `completed` — pre-filter for completed transactions joined with merchants; (2) `merchant_stats` — aggregate by merchant; (3) `category_avg` — average the merchant totals per category. Final SELECT joins stats to avg and filters. Building queries in named steps like this is a hallmark of production-quality SQL.

</details>

| Column | Type | Notes |
|--------|------|-------|
| merchant_id | integer | |
| merchant_name | string | |
| mcc_category | string | |
| total_revenue | double | `ROUND(..., 2)` |
| txn_count | bigint | |
| avg_category_revenue | double | `ROUND(..., 2)` |
| revenue_vs_avg | double | `ROUND(total_revenue / avg_category_revenue, 2)` |

Expected: variable rows, ordered by mcc_category ASC, total_revenue DESC.\
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
## Problem 2: Most Recent Completed Transaction per Account (QUALIFY)

For each account, return only its most recent completed transaction. Use `QUALIFY` to filter after the window function.

<details>
<summary>Hint</summary>

`QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_date DESC, txn_id DESC) = 1`. `QUALIFY` is a DuckDB and Snowflake extension that filters rows based on window function results — no outer query or CTE needed. In PostgreSQL, wrap the window in a subquery or CTE and add `WHERE rn = 1`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| txn_id | integer | |
| txn_date | date | |
| amount | double | |
| merchant_id | integer | |

Expected: one row per account (that has at least one completed txn), ordered by account_id ASC.\
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
## Problem 3: High-Value Users via Chained CTEs

Find users with active accounts whose total completed debit spend is more than twice the overall average. Chain three CTEs: filter active accounts → compute per-user summary → apply the high-value threshold.

<details>
<summary>Hint</summary>

**Step 1** — `active_accounts`: filter accounts WHERE status = 'active'. **Step 2** — `user_summary`: join to transactions, compute txn_count and total_debit per user. **Step 3** — `high_value`: filter users WHERE total_debit > (SELECT AVG(total_debit) * 2 FROM user_summary). Final SELECT joins high_value back to users and user_summary for the output columns.

</details>

| Column | Type | Notes |
|--------|------|-------|
| user_id | integer | |
| name | string | |
| tier | string | |
| country | string | |
| txn_count | bigint | |
| total_debit | double | `ROUND(..., 2)` |

Expected: variable rows, ordered by total_debit DESC.\
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
## Problem 4: Full Monthly Calendar (Recursive CTE)

Generate a complete list of every month from the earliest to the latest transaction date, then LEFT JOIN actual monthly completed transaction totals. Months with no transactions should show 0.

<details>
<summary>Hint</summary>

**Recursive CTE pattern:** `WITH RECURSIVE month_series AS (SELECT DATE_TRUNC('month', MIN(txn_date))::DATE AS month FROM transactions UNION ALL SELECT (month + INTERVAL '1 month')::DATE FROM month_series WHERE month < ...)`. The anchor produces the first month; the recursive step adds one month at a time until the max is reached. LEFT JOIN to actual monthly totals and use COALESCE to replace NULLs with 0.

</details>

| Column | Type | Notes |
|--------|------|-------|
| month | date | |
| txn_count | bigint | 0 for months with no completed transactions |
| total_amount | double | 0.0 for months with no completed transactions |

Expected: one row per month in the calendar range, ordered by month ASC.\
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
## Problem 5: Deduplicate to Latest Active Account per User

Some users have multiple accounts. Return only the most recently opened active account per user. Use a ranking CTE to deduplicate.

<details>
<summary>Hint</summary>

`WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY opened_date DESC, account_id DESC) AS rn FROM accounts WHERE status = 'active') SELECT ... FROM ranked WHERE rn = 1`. This deduplication pattern appears constantly in production data pipelines — entity resolution, slowly changing dimensions, latest-record extraction.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| user_id | integer | |
| account_type | string | |
| opened_date | date | |
| balance | double | |

Expected: one row per user (who has at least one active account), ordered by user_id ASC.\
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

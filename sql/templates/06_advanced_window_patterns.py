def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 06: Advanced Window Patterns

Practice: gap-and-island detection, explicit frame clauses for moving averages,
`NTILE` bucketing, `PERCENTILE_CONT`, and `FIRST_VALUE`/`LAST_VALUE` over an unbounded
frame. These patterns go beyond `ROW_NUMBER`/`RANK`/`LAG` and show up constantly in
production analytics work.
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
from utils.checks.advanced_windows import Checker

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
## Problem 1: Consecutive-Day Activity Streaks (Gap-and-Island)

Find runs of consecutive calendar days on which an account had at least one completed
transaction. Return only streaks of 3 or more consecutive days.

<details>
<summary>Hint</summary>

The classic gap-and-island trick: take the distinct active dates per account, then
subtract a `ROW_NUMBER()` (in days) from each date. Dates that are part of the same
consecutive run all produce the *same* result — that becomes your grouping key.

```sql
WITH active_days AS (
    SELECT DISTINCT account_id, txn_date FROM transactions WHERE status = 'completed'
),
islands AS (
    SELECT account_id, txn_date,
           txn_date - (ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_date))::INT AS island_key
    FROM active_days
)
SELECT account_id, MIN(txn_date) AS streak_start, MAX(txn_date) AS streak_end, COUNT(*) AS streak_length
FROM islands GROUP BY account_id, island_key HAVING COUNT(*) >= 3
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| streak_start | date | |
| streak_end | date | |
| streak_length | bigint | number of consecutive active days |

Expected: variable rows, ordered by streak_length DESC, account_id ASC, streak_start ASC.\
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
## Problem 2: 3-Transaction Trailing Moving Average

For each completed transaction, compute the moving average of `amount` over that
transaction and the 2 preceding it (within the same account), ordered by date.

<details>
<summary>Hint</summary>

`AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_date, txn_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`.
The explicit `ROWS BETWEEN` frame clause is what makes this a *trailing window* of at
most 3 rows, rather than the default frame (`UNBOUNDED PRECEDING` to `CURRENT ROW`)
which would give a running average instead.

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_id | integer | |
| account_id | integer | |
| txn_date | date | |
| amount | double | |
| moving_avg_3 | double | `ROUND(..., 2)`, average of current row + 2 preceding |

Expected: one row per completed transaction, ordered by account_id, txn_date, txn_id.\
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
## Problem 3: Balance Quartiles within Account Type (NTILE)

Bucket every account into a quartile (1 = lowest balances, 4 = highest) based on
`balance`, computed separately within each `account_type`.

<details>
<summary>Hint</summary>

`NTILE(4) OVER (PARTITION BY account_type ORDER BY balance)`. Unlike `RANK`, `NTILE`
divides rows into N roughly-equal-sized buckets regardless of ties or value
distribution — useful for things like decile/quartile segmentation in reporting.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| account_type | string | |
| balance | double | |
| balance_quartile | integer | 1-4, sorted ASC |

Expected: one row per account, ordered by account_type, balance_quartile, account_id.\
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
## Problem 4: Median and 90th Percentile Spend per MCC Category

For each merchant category, compute the median and 90th-percentile completed
transaction amount.

<details>
<summary>Hint</summary>

`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)` is the standard SQL way to
compute an interpolated percentile as a grouped aggregate (not a window function — it
collapses each group to one row, same as `AVG`). Compute both 0.5 and 0.9 in the same
`GROUP BY m.mcc_category` query.

</details>

| Column | Type | Notes |
|--------|------|-------|
| mcc_category | string | sorted ASC |
| median_amount | double | `ROUND(PERCENTILE_CONT(0.5) ..., 2)` |
| p90_amount | double | `ROUND(PERCENTILE_CONT(0.9) ..., 2)` |

Expected: one row per mcc_category, ordered by mcc_category ASC.\
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
## Problem 5: First and Last Completed Transaction per Account

For each account, find the amount of its very first and very latest completed
transaction (by date), in one query, without a self-join.

<details>
<summary>Hint</summary>

`FIRST_VALUE` and `LAST_VALUE` both need the *full* partition as their frame, not the
default "up to current row" frame — otherwise `LAST_VALUE` just returns the current
row's own value. Use
`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` explicitly on both, then
`SELECT DISTINCT` to collapse back to one row per account.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | sorted ASC |
| first_amount | double | amount of the earliest completed transaction |
| last_amount | double | amount of the latest completed transaction |

Expected: one row per account that has at least one completed transaction.\
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

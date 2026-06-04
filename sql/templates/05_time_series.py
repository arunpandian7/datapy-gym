def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 05: Time Series & Date Analysis

Practice: rolling windows, cohort analysis, gap detection, year-over-year comparisons, and anomaly detection.
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
from utils.checks.time_series import Checker

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
## Problem 1: Daily Volume with 7-Day Rolling Average

Compute daily completed transaction count and total amount, along with a 7-day rolling average of the daily amount (including the current day).

<details>
<summary>Hint</summary>

Aggregate to daily totals in a CTE first. Then apply `AVG(daily_amount) OVER (ORDER BY txn_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`. The `ROWS BETWEEN` spec is standard across DuckDB, Snowflake, BigQuery, and PostgreSQL. `RANGE BETWEEN` uses value-based framing — use `ROWS` for row-count-based windows.

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_date | date | |
| daily_count | bigint | |
| daily_amount | double | `ROUND(..., 2)` |
| rolling_7d_avg | double | `ROUND(7-day rolling AVG of daily_amount, 2)` |

Expected: one row per day that has transactions, ordered by txn_date ASC.\
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
## Problem 2: Monthly First-Transaction Cohorts

For each calendar month, count how many users made their very first completed transaction during that month. These are monthly acquisition cohorts.

<details>
<summary>Hint</summary>

CTE to find `MIN(txn_date)` per user (joining transactions to accounts to get user_id). Then group by `DATE_TRUNC('month', first_txn_date)::DATE`. This is the basis for cohort retention analysis — the cohort definition step.

</details>

| Column | Type | Notes |
|--------|------|-------|
| cohort_month | date | `DATE_TRUNC` of first transaction month |
| new_active_users | bigint | COUNT of users whose first txn was in this month |

Expected: one row per month that had at least one new active user, ordered by cohort_month ASC.\
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
## Problem 3: Accounts with Long Inactivity Gaps

Find all instances where an account had a gap of more than 30 days between consecutive completed transactions. Return the account, the gap start date, end date, and the gap length in days.

<details>
<summary>Hint</summary>

Use `LAG(txn_date) OVER (PARTITION BY account_id ORDER BY txn_date, txn_id)` to get the previous transaction's date. Compute `txn_date - prev_txn_date` for the gap. Filter `WHERE gap_days > 30`. In DuckDB and PostgreSQL, date subtraction returns an integer (days). Sort by gap_days DESC to surface the longest gaps first.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| gap_start | date | date of the transaction before the gap |
| gap_end | date | date of the transaction after the gap |
| gap_days | integer | gap_end - gap_start in days |

Expected: variable rows (all gaps > 30 days), ordered by gap_days DESC, account_id ASC.\
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
## Problem 4: Year-over-Year Revenue by MCC Category

For each MCC category and year, compute total completed debit revenue. Add columns for the previous year's revenue and the year-over-year percentage change.

<details>
<summary>Hint</summary>

CTE for annual totals per category. Outer query adds `LAG(total_revenue) OVER (PARTITION BY mcc_category ORDER BY year)`. YoY % = `(current - prev) / prev * 100`. The first year per category will have NULL for the lag and pct change columns. `EXTRACT(YEAR FROM txn_date)::INT` gives integer year in DuckDB and PostgreSQL. Snowflake: `YEAR(txn_date)`. BigQuery: `EXTRACT(YEAR FROM txn_date)`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| mcc_category | string | |
| year | integer | `EXTRACT(YEAR FROM txn_date)::INT` |
| total_revenue | double | `ROUND(..., 2)` |
| prev_year_revenue | double | `ROUND(..., 2)`, NULL for first year |
| yoy_pct_change | double | `ROUND(..., 2)`, NULL for first year |

Expected: rows ordered by mcc_category ASC, year ASC.\
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
## Problem 5: Transaction Volume Anomalies

Identify days where the completed transaction count was more than 2 standard deviations above the daily mean. Return the date, count, total amount, and the global mean and stddev for context.

<details>
<summary>Hint</summary>

CTE for daily stats. CROSS JOIN with a single-row stats CTE (`AVG`, `STDDEV`). Filter `WHERE daily_count > mean_daily_count + 2 * std_daily_count`. `STDDEV()` (population stddev) works the same in DuckDB, Snowflake, BigQuery, and PostgreSQL. `CROSS JOIN` of a scalar CTE is idiomatic for broadcasting constants.

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_date | date | |
| daily_count | bigint | |
| daily_amount | double | `ROUND(..., 2)` |
| mean_daily_count | double | `ROUND(..., 2)` — global average |
| std_daily_count | double | `ROUND(..., 2)` — global stddev |

Expected: variable rows (anomaly days only), ordered by daily_count DESC.\
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

def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 08: Pivoting & Data Quality

Practice: `PIVOT`/`UNPIVOT` for reshaping data, and the data-quality audit queries every
senior engineer writes before trusting a new source — duplicate detection, referential
integrity checks, and null-completeness profiling.
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
from utils.checks.pivot_and_data_quality import Checker

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
## Problem 1: Completed Transactions per Year, Pivoted by Account Type

Count completed transactions per year, with one column per `account_type`
(`checking`, `savings`, `credit`) instead of one row per (year, account_type) pair.

<details>
<summary>Hint</summary>

DuckDB's `PIVOT` statement wraps a subquery, names the column whose distinct values
become new columns (`ON account_type`), and an explicit value list to pin down both
which values appear and their column order:

```sql
PIVOT (
    SELECT a.account_type, EXTRACT(year FROM t.txn_date)::INT AS yr, t.txn_id
    FROM transactions t JOIN accounts a ON t.account_id = a.account_id
    WHERE t.status = 'completed'
) ON account_type IN ('checking', 'savings', 'credit') USING COUNT(txn_id)
GROUP BY yr
```

Without the explicit `IN (...)` list, the column order isn't guaranteed — pin it down
whenever a pivot's output feeds something order-sensitive.

</details>

| Column | Type | Notes |
|--------|------|-------|
| yr | integer | sorted ASC |
| checking | bigint | |
| savings | bigint | |
| credit | bigint | |

Expected: one row per year (2022, 2023, 2024) present in the data.\
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
## Problem 2: Unpivot Back to Long Form

Take the pivoted table from Problem 1 and reshape it back to long form: one row per
(year, account_type) pair with the count as a value column.

<details>
<summary>Hint</summary>

```sql
WITH pivoted AS ( ... same PIVOT as Problem 1 ... )
UNPIVOT pivoted ON checking, savings, credit INTO NAME account_type VALUE txn_count
```

`UNPIVOT ... ON col1, col2, col3` lists the columns to melt; `INTO NAME ... VALUE ...`
names the resulting key and value columns. This round-trip (long → wide → long) is
exactly what a BI tool does internally when you drag a dimension onto a pivot table.

</details>

| Column | Type | Notes |
|--------|------|-------|
| yr | integer | sorted ASC |
| account_type | string | sorted ASC within year |
| txn_count | bigint | |

Expected: 9 rows (3 years × 3 account types).\
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
## Problem 3: Potential Duplicate Transactions

Flag groups of transactions that share the same account, merchant, and date — a common
"possible double-charge" rule before trusting transaction counts in downstream reports.

<details>
<summary>Hint</summary>

`GROUP BY account_id, merchant_id, txn_date HAVING COUNT(*) > 1`. This is the same
shape as detecting duplicate rows on any natural key: group by the columns that should
*identify* a unique real-world event, and surface any group with more than one row.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| merchant_id | integer | |
| txn_date | date | |
| dup_count | bigint | sorted DESC, then by account_id, merchant_id, txn_date ASC |

Expected: a handful of rows — true duplicates should be rare in clean data, which is
exactly why finding even 3 is worth flagging upstream.\
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
## Problem 4: Referential Integrity — Orphaned Transactions

Verify that every transaction's `account_id` and `merchant_id` actually exist in their
parent tables. This is the kind of check that should run as a pipeline gate, not just
once during development.

<details>
<summary>Hint</summary>

```sql
SELECT t.txn_id, t.account_id, t.merchant_id
FROM transactions t
WHERE t.account_id NOT IN (SELECT account_id FROM accounts)
   OR t.merchant_id NOT IN (SELECT merchant_id FROM merchants)
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_id | integer | sorted ASC |
| account_id | integer | |
| merchant_id | integer | |

Expected: 0 rows — this dataset's foreign keys are fully consistent. The query itself
is the deliverable: it's the gate you'd wire into a pipeline to catch a future regression.\
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
## Problem 5: Null Completeness Profile

Build a generic completeness report: for each of `account_id`, `merchant_id`,
`amount`, `txn_date`, and `status` in `transactions`, report how many rows are NULL out
of the total.

<details>
<summary>Hint</summary>

`COUNT(*)` counts all rows; `COUNT(col)` skips NULLs — so `COUNT(*) - COUNT(col)` is
the null count for that column. Stack one such row per column with `UNION ALL`:

```sql
SELECT 'account_id' AS column_name, COUNT(*) - COUNT(account_id) AS null_count, COUNT(*) AS total_rows
FROM transactions
UNION ALL
SELECT 'merchant_id', COUNT(*) - COUNT(merchant_id), COUNT(*) FROM transactions
UNION ALL
...
```

This "one row per column" profile is the basis of most automated data-quality
dashboards — each column gets reduced to a few summary stats and compared against a
threshold.

</details>

| Column | Type | Notes |
|--------|------|-------|
| column_name | string | sorted ASC |
| null_count | bigint | |
| total_rows | bigint | |

Expected: 5 rows, all with `null_count = 0` — a fully complete table.\
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

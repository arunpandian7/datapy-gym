def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 03: Joins

Practice: `INNER JOIN`, `LEFT JOIN`, anti-joins, self-joins, and multi-table join chains.
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
from utils.checks.joins import Checker

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
## Problem 1: Full Transaction Details

Enrich every transaction with user info (name, country, tier), account type, and merchant info (name, category). Return all transactions, ordered by txn_id.

<details>
<summary>Hint</summary>

Chain four INNER JOINs: `transactions → accounts` (on account_id) `→ users` (on user_id) `→ merchants` (on merchant_id). Use `u.name AS user_name` and `m.name AS merchant_name` to disambiguate. `USING (col)` works when column names match; `ON t.account_id = a.account_id` is more explicit — interviewers appreciate you knowing both.

</details>

| Column | Type | Notes |
|--------|------|-------|
| txn_id | integer | |
| txn_date | date | |
| amount | double | |
| txn_type | string | |
| status | string | |
| user_name | string | `u.name AS user_name` |
| country | string | |
| tier | string | |
| account_type | string | |
| merchant_name | string | `m.name AS merchant_name` |
| mcc_category | string | |

Expected: all **20,000 transactions**, ordered by txn_id.\
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
## Problem 2: Users Who Have Never Made a Transaction

Find users with no transactions whatsoever (not even failed or pending). Return their profile details sorted by user_id.

<details>
<summary>Hint</summary>

Anti-join pattern. LEFT JOIN `users → accounts → transactions`, then filter `WHERE t.txn_id IS NULL`. This is the standard anti-join idiom across all SQL dialects. Alternatively: `WHERE user_id NOT IN (SELECT DISTINCT a.user_id FROM accounts a JOIN transactions t ON a.account_id = t.account_id)`.

</details>

| Column | Type | Notes |
|--------|------|-------|
| user_id | integer | |
| name | string | |
| email | string | |
| country | string | |
| tier | string | |

Expected: variable rows, ordered by user_id ASC.\
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
## Problem 3: Revenue by MCC Category for Premium/Business Users

Among premium and business tier users only, compute transaction count and total revenue for each MCC category (completed transactions only). Sort by mcc_category, then tier.

<details>
<summary>Hint</summary>

Four-table join. Filter `u.tier IN ('premium', 'business')` and `t.status = 'completed'`. Group by `mcc_category`, `tier`. This is a filtered aggregation via join — no subquery needed.

</details>

| Column | Type | Notes |
|--------|------|-------|
| mcc_category | string | |
| tier | string | |
| txn_count | bigint | `COUNT(*)` |
| total_revenue | double | `ROUND(SUM(amount), 2)` |

Expected: up to **16 rows** (8 categories × 2 tiers), ordered by mcc_category ASC, tier ASC.\
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
## Problem 4: Accounts with Both Debit and Credit Transactions

Find accounts that have at least one completed debit AND at least one completed credit transaction. Return account details sorted by account_id.

<details>
<summary>Hint</summary>

Use subquery intersection: `WHERE account_id IN (SELECT ... WHERE txn_type = 'debit') AND account_id IN (SELECT ... WHERE txn_type = 'credit')`. Alternative approach (self-join): `JOIN transactions d ON ... JOIN transactions c ON d.account_id = c.account_id AND d.txn_type = 'debit' AND c.txn_type = 'credit'` — remember `DISTINCT` to avoid row explosion.

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | |
| account_type | string | |
| user_id | integer | |

Expected: variable rows, ordered by account_id ASC.\
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
## Problem 5: Users Whose Spend Exceeds Their Country Average

For completed debit transactions, find users whose total spend is above the average total spend for their country. Show each user's total spend alongside their country's average.

<details>
<summary>Hint</summary>

Two CTEs: first compute per-user total spend, then compute per-country average of those totals. JOIN them on `country` and filter `WHERE total_spend > country_avg_spend`. This pattern (aggregate → re-aggregate → compare) is very common in analytics interviews.

</details>

| Column | Type | Notes |
|--------|------|-------|
| user_id | integer | |
| name | string | |
| country | string | |
| total_spend | double | `ROUND(..., 2)` |
| country_avg_spend | double | `ROUND(..., 2)` |

Expected: variable rows, ordered by country ASC, total_spend DESC.\
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

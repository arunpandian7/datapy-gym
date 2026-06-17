def cells() -> list[tuple[str, str]]:
    """Returns (cell_type, source) pairs. cell_type is 'markdown' or 'code'."""
    return [
        # ── Title ────────────────────────────────────────────────────────────
        (
            "markdown",
            """\
# SQL Gym — 07: Set Operations & Subqueries

Practice: correlated `EXISTS`/`NOT EXISTS` (the semi-join and anti-join patterns
underneath `IN`/`NOT IN` and `LEFT JOIN ... IS NULL`), `INTERSECT`/`EXCEPT` set
operations, and the `> ALL` / `> ANY` quantified comparison operators.
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
from utils.checks.set_ops_subqueries import Checker

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
## Problem 1: Merchants with No Big-Ticket Transactions (NOT EXISTS)

Find merchants who have never had a single completed transaction over $2000.

<details>
<summary>Hint</summary>

A correlated `NOT EXISTS` is an anti-join: for every merchant row, the subquery checks
whether a qualifying transaction exists *for that specific merchant*
(`t.merchant_id = m.merchant_id` ties the subquery back to the outer row). It is
equivalent to a `LEFT JOIN ... WHERE t.txn_id IS NULL` anti-join, but reads more
directly as "a row for which this condition never holds."

```sql
SELECT m.merchant_id, m.name, m.mcc_category
FROM merchants m
WHERE NOT EXISTS (
    SELECT 1 FROM transactions t
    WHERE t.merchant_id = m.merchant_id AND t.status = 'completed' AND t.amount > 2000
)
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| merchant_id | integer | sorted ASC |
| name | string | |
| mcc_category | string | |

Expected: 100 of the 200 merchants.\
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
## Problem 2: Users with a Large Transaction (EXISTS)

Find users who have made at least one completed transaction over $1000, through any of
their accounts.

<details>
<summary>Hint</summary>

`EXISTS` is the semi-join counterpart to `NOT EXISTS` — it stops at the first matching
row per outer record instead of counting or joining all matches, which is often faster
than a `JOIN` + `DISTINCT` for a pure existence check.

```sql
SELECT u.user_id, u.name, u.country, u.tier
FROM users u
WHERE EXISTS (
    SELECT 1 FROM accounts a
    JOIN transactions t ON a.account_id = t.account_id
    WHERE a.user_id = u.user_id AND t.status = 'completed' AND t.amount > 1000
)
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| user_id | integer | sorted ASC |
| name | string | |
| country | string | |
| tier | string | |

Expected: one row per qualifying user — no duplicates even if they have multiple
qualifying transactions, which a `JOIN` without `DISTINCT` would produce.\
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
## Problem 3: Categories Purchased by Both Premium and Business Tiers (INTERSECT)

Find every `mcc_category` that has at least one completed transaction from a
`premium`-tier user **and** at least one from a `business`-tier user.

<details>
<summary>Hint</summary>

`INTERSECT` returns only rows that appear in *both* result sets — it deduplicates
automatically, so no `DISTINCT` is needed. Run the same category-fetching query twice,
once filtered to `tier = 'premium'` and once to `tier = 'business'`, and intersect them:

```sql
SELECT m.mcc_category FROM transactions t JOIN accounts a ... WHERE u.tier = 'premium' ...
INTERSECT
SELECT m.mcc_category FROM transactions t JOIN accounts a ... WHERE u.tier = 'business' ...
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| mcc_category | string | sorted ASC |

Expected: all 8 categories — both tiers are active spenders across every category in
this dataset.\
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
## Problem 4: Accounts with Debits but Never a Credit (EXCEPT)

Find accounts that have at least one completed debit transaction, but zero completed
credit transactions.

<details>
<summary>Hint</summary>

`EXCEPT` returns rows from the first query that do **not** appear in the second —
exactly "debit accounts minus credit accounts":

```sql
SELECT account_id FROM (SELECT DISTINCT account_id FROM transactions WHERE txn_type = 'debit' AND status = 'completed')
EXCEPT
SELECT account_id FROM (SELECT DISTINCT account_id FROM transactions WHERE txn_type = 'credit' AND status = 'completed')
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| account_id | integer | sorted ASC |

Expected: a small number of accounts — most accounts have both debit and credit
activity in this dataset, so this isolates the rare exceptions.\
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
## Problem 5: Merchants Outselling Every Restaurant (> ALL)

Find merchants (outside the `restaurants` category) whose total completed revenue
exceeds **every single** restaurant's total revenue.

<details>
<summary>Hint</summary>

`> ALL (subquery)` means "greater than the maximum value the subquery returns" —
equivalent to `> (SELECT MAX(...) ...)`, but `ALL` generalizes to other comparisons too
(`< ALL` means "less than the minimum"). Build a `merchant_revenue` CTE once, then
compare against the slice of it filtered to `mcc_category = 'restaurants'`:

```sql
WITH merchant_revenue AS (
    SELECT m.merchant_id, m.name, m.mcc_category, ROUND(SUM(t.amount), 2) AS total_revenue
    FROM transactions t JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.status = 'completed'
    GROUP BY m.merchant_id, m.name, m.mcc_category
)
SELECT merchant_id, name, mcc_category, total_revenue
FROM merchant_revenue
WHERE mcc_category != 'restaurants'
AND total_revenue > ALL (SELECT total_revenue FROM merchant_revenue WHERE mcc_category = 'restaurants')
```

</details>

| Column | Type | Notes |
|--------|------|-------|
| merchant_id | integer | |
| name | string | |
| mcc_category | string | |
| total_revenue | double | `ROUND(..., 2)`, sorted DESC |

Expected: 31 merchants — every one of them out-earns the single highest-grossing
restaurant.\
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

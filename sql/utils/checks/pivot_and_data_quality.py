import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
PIVOT (
    SELECT a.account_type, EXTRACT(year FROM t.txn_date)::INT AS yr, t.txn_id
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.status = 'completed'
) ON account_type IN ('checking', 'savings', 'credit') USING COUNT(txn_id)
GROUP BY yr
ORDER BY yr
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Completed Transactions per Year, Pivoted by Account Type", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
WITH pivoted AS (
    PIVOT (
        SELECT a.account_type, EXTRACT(year FROM t.txn_date)::INT AS yr, t.txn_id
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        WHERE t.status = 'completed'
    ) ON account_type IN ('checking', 'savings', 'credit') USING COUNT(txn_id)
    GROUP BY yr
)
UNPIVOT pivoted ON checking, savings, credit INTO NAME account_type VALUE txn_count
ORDER BY yr, account_type
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Unpivot Back to Long Form", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
SELECT account_id, merchant_id, txn_date, COUNT(*) AS dup_count
FROM transactions
GROUP BY account_id, merchant_id, txn_date
HAVING COUNT(*) > 1
ORDER BY dup_count DESC, account_id, merchant_id, txn_date
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Potential Duplicate Transactions", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
SELECT t.txn_id, t.account_id, t.merchant_id
FROM transactions t
WHERE t.account_id NOT IN (SELECT account_id FROM accounts)
   OR t.merchant_id NOT IN (SELECT merchant_id FROM merchants)
ORDER BY t.txn_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Referential Integrity — Orphaned Transactions", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
SELECT 'account_id' AS column_name, COUNT(*) - COUNT(account_id) AS null_count, COUNT(*) AS total_rows FROM transactions
UNION ALL
SELECT 'merchant_id', COUNT(*) - COUNT(merchant_id), COUNT(*) FROM transactions
UNION ALL
SELECT 'amount', COUNT(*) - COUNT(amount), COUNT(*) FROM transactions
UNION ALL
SELECT 'txn_date', COUNT(*) - COUNT(txn_date), COUNT(*) FROM transactions
UNION ALL
SELECT 'status', COUNT(*) - COUNT(status), COUNT(*) FROM transactions
ORDER BY column_name
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Null Completeness Profile", ordered=True)

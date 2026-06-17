import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
SELECT m.merchant_id, m.name, m.mcc_category
FROM merchants m
WHERE NOT EXISTS (
    SELECT 1 FROM transactions t
    WHERE t.merchant_id = m.merchant_id AND t.status = 'completed' AND t.amount > 2000
)
ORDER BY m.merchant_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Merchants with No Big-Ticket Transactions (NOT EXISTS)", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
SELECT u.user_id, u.name, u.country, u.tier
FROM users u
WHERE EXISTS (
    SELECT 1 FROM accounts a
    JOIN transactions t ON a.account_id = t.account_id
    WHERE a.user_id = u.user_id AND t.status = 'completed' AND t.amount > 1000
)
ORDER BY u.user_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Users with a Large Transaction (EXISTS)", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
SELECT m.mcc_category
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
JOIN users u ON a.user_id = u.user_id
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE u.tier = 'premium' AND t.status = 'completed'
INTERSECT
SELECT m.mcc_category
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
JOIN users u ON a.user_id = u.user_id
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE u.tier = 'business' AND t.status = 'completed'
ORDER BY mcc_category
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Categories Purchased by Both Premium and Business Tiers (INTERSECT)", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
SELECT account_id FROM (
    SELECT DISTINCT account_id FROM transactions WHERE txn_type = 'debit' AND status = 'completed'
)
EXCEPT
SELECT account_id FROM (
    SELECT DISTINCT account_id FROM transactions WHERE txn_type = 'credit' AND status = 'completed'
)
ORDER BY account_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Accounts with Debits but Never a Credit (EXCEPT)", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
WITH merchant_revenue AS (
    SELECT m.merchant_id, m.name, m.mcc_category,
           ROUND(SUM(t.amount), 2) AS total_revenue
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.status = 'completed'
    GROUP BY m.merchant_id, m.name, m.mcc_category
)
SELECT merchant_id, name, mcc_category, total_revenue
FROM merchant_revenue
WHERE mcc_category != 'restaurants'
AND total_revenue > ALL (
    SELECT total_revenue FROM merchant_revenue WHERE mcc_category = 'restaurants'
)
ORDER BY total_revenue DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Merchants Outselling Every Restaurant (> ALL)", ordered=True)

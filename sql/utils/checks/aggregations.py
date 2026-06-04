import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
            SELECT m.mcc_category,
                   ROUND(SUM(t.amount), 2) AS total_spent
            FROM transactions t
            JOIN merchants m ON t.merchant_id = m.merchant_id
            WHERE t.txn_type = 'debit' AND t.status = 'completed'
            GROUP BY m.mcc_category
            ORDER BY total_spent DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Total spending by MCC category", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
            SELECT m.merchant_id, m.name, m.mcc_category,
                   COUNT(*) AS txn_count,
                   ROUND(SUM(t.amount), 2) AS total_amount
            FROM transactions t
            JOIN merchants m ON t.merchant_id = m.merchant_id
            GROUP BY m.merchant_id, m.name, m.mcc_category
            ORDER BY txn_count DESC
            LIMIT 10
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Top 10 merchants by transaction count", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
            SELECT DATE_TRUNC('month', t.txn_date)::DATE AS month,
                   a.account_type,
                   COUNT(*) AS txn_count,
                   ROUND(SUM(t.amount), 2) AS total_amount
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            WHERE t.status = 'completed'
            GROUP BY DATE_TRUNC('month', t.txn_date)::DATE, a.account_type
            ORDER BY month, account_type
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Monthly txn count and total amount by account type", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
            SELECT u.user_id, u.name, u.country, u.tier,
                   ROUND(AVG(t.amount), 2) AS avg_txn_amount,
                   COUNT(*) AS txn_count
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            JOIN users u ON a.user_id = u.user_id
            WHERE t.txn_type = 'debit' AND t.status = 'completed'
            GROUP BY u.user_id, u.name, u.country, u.tier
            HAVING AVG(t.amount) > 500
            ORDER BY avg_txn_amount DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Users with average completed debit > $500", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
            SELECT u.country,
                   COUNT(DISTINCT u.user_id) AS user_count,
                   COUNT(DISTINCT a.account_id) AS account_count,
                   COUNT(DISTINCT t.txn_id) AS txn_count,
                   ROUND(SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END), 2) AS total_completed_amount
            FROM users u
            LEFT JOIN accounts a ON u.user_id = a.user_id
            LEFT JOIN transactions t ON a.account_id = t.account_id
            GROUP BY u.country
            ORDER BY user_count DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Country-level summary", ordered=True)

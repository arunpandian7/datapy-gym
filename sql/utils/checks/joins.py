import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
            SELECT t.txn_id, t.txn_date, t.amount, t.txn_type, t.status,
                   u.name AS user_name, u.country, u.tier,
                   a.account_type,
                   m.name AS merchant_name, m.mcc_category
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            JOIN users u ON a.user_id = u.user_id
            JOIN merchants m ON t.merchant_id = m.merchant_id
            ORDER BY t.txn_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Full transaction details (all 4 tables)", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
            SELECT u.user_id, u.name, u.email, u.country, u.tier
            FROM users u
            LEFT JOIN accounts a ON u.user_id = a.user_id
            LEFT JOIN transactions t ON a.account_id = t.account_id
            WHERE t.txn_id IS NULL
            ORDER BY u.user_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Users who have never made a transaction", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
            SELECT m.mcc_category, u.tier,
                   COUNT(*) AS txn_count,
                   ROUND(SUM(t.amount), 2) AS total_revenue
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            JOIN users u ON a.user_id = u.user_id
            JOIN merchants m ON t.merchant_id = m.merchant_id
            WHERE u.tier IN ('premium', 'business') AND t.status = 'completed'
            GROUP BY m.mcc_category, u.tier
            ORDER BY mcc_category, tier
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Revenue by MCC category for premium and business tiers", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
            SELECT a.account_id, a.account_type, a.user_id
            FROM accounts a
            WHERE a.account_id IN (
                SELECT account_id FROM transactions
                WHERE txn_type = 'debit' AND status = 'completed'
            )
            AND a.account_id IN (
                SELECT account_id FROM transactions
                WHERE txn_type = 'credit' AND status = 'completed'
            )
            ORDER BY a.account_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Accounts with both debit and credit completed transactions", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
            WITH user_spend AS (
                SELECT u.user_id, u.name, u.country,
                       ROUND(SUM(t.amount), 2) AS total_spend
                FROM transactions t
                JOIN accounts a ON t.account_id = a.account_id
                JOIN users u ON a.user_id = u.user_id
                WHERE t.txn_type = 'debit' AND t.status = 'completed'
                GROUP BY u.user_id, u.name, u.country
            ),
            country_avg AS (
                SELECT country, ROUND(AVG(total_spend), 2) AS country_avg_spend
                FROM user_spend
                GROUP BY country
            )
            SELECT us.user_id, us.name, us.country, us.total_spend, ca.country_avg_spend
            FROM user_spend us
            JOIN country_avg ca ON us.country = ca.country
            WHERE us.total_spend > ca.country_avg_spend
            ORDER BY us.country, us.total_spend DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Users whose spend exceeds their country average", ordered=True)

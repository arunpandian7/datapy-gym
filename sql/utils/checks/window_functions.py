import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
            SELECT t.txn_id, t.account_id, t.txn_date, t.amount, t.txn_type,
                   ROUND(SUM(CASE WHEN t.txn_type = 'credit' THEN t.amount ELSE -t.amount END)
                         OVER (PARTITION BY t.account_id
                               ORDER BY t.txn_date, t.txn_id
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS running_balance
            FROM transactions t
            WHERE t.status = 'completed'
            ORDER BY t.account_id, t.txn_date, t.txn_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Running balance per account", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
            WITH merchant_revenue AS (
                SELECT m.merchant_id, m.name, m.mcc_category,
                       ROUND(SUM(t.amount), 2) AS total_revenue
                FROM transactions t
                JOIN merchants m ON t.merchant_id = m.merchant_id
                WHERE t.status = 'completed'
                GROUP BY m.merchant_id, m.name, m.mcc_category
            )
            SELECT merchant_id, name, mcc_category, total_revenue,
                   DENSE_RANK() OVER (PARTITION BY mcc_category ORDER BY total_revenue DESC) AS revenue_rank
            FROM merchant_revenue
            QUALIFY revenue_rank <= 3
            ORDER BY mcc_category, revenue_rank
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Top 3 merchants by revenue per MCC category", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
            WITH monthly AS (
                SELECT u.country,
                       DATE_TRUNC('month', t.txn_date)::DATE AS month,
                       ROUND(SUM(t.amount), 2) AS monthly_spend
                FROM transactions t
                JOIN accounts a ON t.account_id = a.account_id
                JOIN users u ON a.user_id = u.user_id
                WHERE t.txn_type = 'debit' AND t.status = 'completed'
                GROUP BY u.country, DATE_TRUNC('month', t.txn_date)::DATE
            )
            SELECT country, month, monthly_spend,
                   LAG(monthly_spend, 1) OVER (PARTITION BY country ORDER BY month) AS prev_month_spend,
                   ROUND(
                       (monthly_spend - LAG(monthly_spend, 1) OVER (PARTITION BY country ORDER BY month))
                       / LAG(monthly_spend, 1) OVER (PARTITION BY country ORDER BY month) * 100,
                   2) AS mom_pct_change
            FROM monthly
            ORDER BY country, month
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Month-over-month spend change per country", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
            WITH user_spend AS (
                SELECT u.user_id, u.name, u.country, u.tier,
                       ROUND(SUM(t.amount), 2) AS total_spend
                FROM transactions t
                JOIN accounts a ON t.account_id = a.account_id
                JOIN users u ON a.user_id = u.user_id
                WHERE t.txn_type = 'debit' AND t.status = 'completed'
                GROUP BY u.user_id, u.name, u.country, u.tier
            )
            SELECT user_id, name, country, tier, total_spend,
                   ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_spend DESC) AS spend_rank
            FROM user_spend
            QUALIFY spend_rank <= 2
            ORDER BY country, spend_rank
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Top 2 users by spend per country", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
            SELECT t.txn_id, m.mcc_category, t.amount,
                   ROUND(PERCENT_RANK() OVER (PARTITION BY m.mcc_category ORDER BY t.amount), 4) AS pct_rank
            FROM transactions t
            JOIN merchants m ON t.merchant_id = m.merchant_id
            WHERE t.status = 'completed'
            ORDER BY m.mcc_category, t.amount
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Percentile rank of txn amounts within MCC category", ordered=False)

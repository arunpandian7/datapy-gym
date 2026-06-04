import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
WITH completed AS (
    SELECT t.*, m.mcc_category, m.name AS merchant_name
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.status = 'completed'
),
merchant_stats AS (
    SELECT merchant_id, merchant_name, mcc_category,
           COUNT(*) AS txn_count,
           ROUND(SUM(amount), 2) AS total_revenue
    FROM completed
    GROUP BY merchant_id, merchant_name, mcc_category
),
category_avg AS (
    SELECT mcc_category,
           ROUND(AVG(total_revenue), 2) AS avg_category_revenue
    FROM merchant_stats
    GROUP BY mcc_category
)
SELECT ms.merchant_id, ms.merchant_name, ms.mcc_category,
       ms.total_revenue, ms.txn_count,
       ca.avg_category_revenue,
       ROUND(ms.total_revenue / ca.avg_category_revenue, 2) AS revenue_vs_avg
FROM merchant_stats ms
JOIN category_avg ca ON ms.mcc_category = ca.mcc_category
WHERE ms.total_revenue > ca.avg_category_revenue
ORDER BY ms.mcc_category, ms.total_revenue DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Above-average merchants per category", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
SELECT t.account_id, t.txn_id, t.txn_date, t.amount, t.merchant_id
FROM transactions t
WHERE t.status = 'completed'
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.account_id ORDER BY t.txn_date DESC, t.txn_id DESC) = 1
ORDER BY t.account_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Most recent completed transaction per account", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
WITH active_accounts AS (
    SELECT account_id, user_id FROM accounts WHERE status = 'active'
),
user_summary AS (
    SELECT a.user_id,
           COUNT(DISTINCT t.txn_id) AS txn_count,
           ROUND(SUM(CASE WHEN t.txn_type = 'debit' AND t.status = 'completed'
                          THEN t.amount ELSE 0 END), 2) AS total_debit
    FROM active_accounts a
    JOIN transactions t ON a.account_id = t.account_id
    GROUP BY a.user_id
),
high_value AS (
    SELECT user_id FROM user_summary
    WHERE total_debit > (SELECT AVG(total_debit) * 2 FROM user_summary)
)
SELECT u.user_id, u.name, u.tier, u.country,
       s.txn_count, s.total_debit
FROM high_value h
JOIN users u ON h.user_id = u.user_id
JOIN user_summary s ON h.user_id = s.user_id
ORDER BY s.total_debit DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: High-value users with active accounts", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
WITH RECURSIVE month_series AS (
    SELECT DATE_TRUNC('month', MIN(txn_date))::DATE AS month
    FROM transactions
    UNION ALL
    SELECT (month + INTERVAL '1 month')::DATE
    FROM month_series
    WHERE month < DATE_TRUNC('month', (SELECT MAX(txn_date) FROM transactions))::DATE
),
monthly_totals AS (
    SELECT DATE_TRUNC('month', txn_date)::DATE AS month,
           COUNT(*) AS txn_count,
           ROUND(SUM(amount), 2) AS total_amount
    FROM transactions
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('month', txn_date)::DATE
)
SELECT ms.month,
       COALESCE(mt.txn_count, 0) AS txn_count,
       COALESCE(mt.total_amount, 0.0) AS total_amount
FROM month_series ms
LEFT JOIN monthly_totals mt ON ms.month = mt.month
ORDER BY ms.month
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Complete monthly calendar with transaction totals", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
WITH ranked AS (
    SELECT account_id, user_id, account_type, opened_date, balance,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY opened_date DESC, account_id DESC) AS rn
    FROM accounts
    WHERE status = 'active'
)
SELECT account_id, user_id, account_type, opened_date, balance
FROM ranked
WHERE rn = 1
ORDER BY user_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Most recently opened active account per user", ordered=True)

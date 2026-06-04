import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
WITH daily AS (
    SELECT txn_date,
           COUNT(*) AS daily_count,
           ROUND(SUM(amount), 2) AS daily_amount
    FROM transactions
    WHERE status = 'completed'
    GROUP BY txn_date
)
SELECT txn_date, daily_count, daily_amount,
       ROUND(AVG(daily_amount) OVER (
           ORDER BY txn_date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ), 2) AS rolling_7d_avg
FROM daily
ORDER BY txn_date
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Daily completed volume with 7-day rolling average", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
WITH first_txn AS (
    SELECT a.user_id, MIN(t.txn_date) AS first_txn_date
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.status = 'completed'
    GROUP BY a.user_id
)
SELECT DATE_TRUNC('month', first_txn_date)::DATE AS cohort_month,
       COUNT(*) AS new_active_users
FROM first_txn
GROUP BY DATE_TRUNC('month', first_txn_date)::DATE
ORDER BY cohort_month
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: Monthly first-transaction cohort sizes", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
WITH ordered_txns AS (
    SELECT account_id, txn_date, txn_id,
           LAG(txn_date) OVER (PARTITION BY account_id ORDER BY txn_date, txn_id) AS prev_txn_date
    FROM transactions
    WHERE status = 'completed'
),
gaps AS (
    SELECT account_id,
           prev_txn_date AS gap_start,
           txn_date AS gap_end,
           (txn_date - prev_txn_date) AS gap_days
    FROM ordered_txns
    WHERE prev_txn_date IS NOT NULL
      AND (txn_date - prev_txn_date) > 30
)
SELECT account_id, gap_start, gap_end, gap_days
FROM gaps
ORDER BY gap_days DESC, account_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Accounts with transaction gaps > 30 days", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
WITH yearly AS (
    SELECT m.mcc_category,
           EXTRACT(YEAR FROM t.txn_date)::INT AS year,
           ROUND(SUM(t.amount), 2) AS total_revenue
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.status = 'completed' AND t.txn_type = 'debit'
    GROUP BY m.mcc_category, EXTRACT(YEAR FROM t.txn_date)::INT
)
SELECT mcc_category, year, total_revenue,
       LAG(total_revenue) OVER (PARTITION BY mcc_category ORDER BY year) AS prev_year_revenue,
       ROUND(
           (total_revenue - LAG(total_revenue) OVER (PARTITION BY mcc_category ORDER BY year))
           / LAG(total_revenue) OVER (PARTITION BY mcc_category ORDER BY year) * 100,
       2) AS yoy_pct_change
FROM yearly
ORDER BY mcc_category, year
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Year-over-year revenue by MCC category", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
WITH daily_stats AS (
    SELECT txn_date,
           COUNT(*) AS daily_count,
           ROUND(SUM(amount), 2) AS daily_amount
    FROM transactions
    WHERE status = 'completed'
    GROUP BY txn_date
),
global_stats AS (
    SELECT ROUND(AVG(daily_count), 2) AS mean_daily_count,
           ROUND(STDDEV(daily_count), 2) AS std_daily_count
    FROM daily_stats
)
SELECT d.txn_date, d.daily_count, d.daily_amount,
       g.mean_daily_count, g.std_daily_count
FROM daily_stats d
CROSS JOIN global_stats g
WHERE d.daily_count > g.mean_daily_count + 2 * g.std_daily_count
ORDER BY d.daily_count DESC
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: Anomaly days exceeding mean + 2×stddev", ordered=True)

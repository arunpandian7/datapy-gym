import duckdb
import pandas as pd
from utils import check


class Checker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def p1(self, solution):
        expected_df = self.conn.execute("""
WITH active_days AS (
    SELECT DISTINCT account_id, txn_date
    FROM transactions
    WHERE status = 'completed'
),
islands AS (
    SELECT account_id, txn_date,
           txn_date - (ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_date))::INT AS island_key
    FROM active_days
)
SELECT account_id,
       MIN(txn_date) AS streak_start,
       MAX(txn_date) AS streak_end,
       COUNT(*) AS streak_length
FROM islands
GROUP BY account_id, island_key
HAVING COUNT(*) >= 3
ORDER BY streak_length DESC, account_id, streak_start
        """).df()
        return check(solution, expected_df, self.conn, problem="P1: Consecutive-Day Activity Streaks (Gap-and-Island)", ordered=True)

    def p2(self, solution):
        expected_df = self.conn.execute("""
SELECT t.txn_id, t.account_id, t.txn_date, t.amount,
       ROUND(AVG(t.amount) OVER (
           PARTITION BY t.account_id
           ORDER BY t.txn_date, t.txn_id
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ), 2) AS moving_avg_3
FROM transactions t
WHERE t.status = 'completed'
ORDER BY t.account_id, t.txn_date, t.txn_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P2: 3-Transaction Trailing Moving Average", ordered=True)

    def p3(self, solution):
        expected_df = self.conn.execute("""
SELECT account_id, account_type, balance,
       NTILE(4) OVER (PARTITION BY account_type ORDER BY balance) AS balance_quartile
FROM accounts
ORDER BY account_type, balance_quartile, account_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P3: Balance Quartiles within Account Type (NTILE)", ordered=True)

    def p4(self, solution):
        expected_df = self.conn.execute("""
SELECT m.mcc_category,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount), 2) AS median_amount,
       ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY t.amount), 2) AS p90_amount
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE t.status = 'completed'
GROUP BY m.mcc_category
ORDER BY m.mcc_category
        """).df()
        return check(solution, expected_df, self.conn, problem="P4: Median and 90th Percentile Spend per MCC Category", ordered=True)

    def p5(self, solution):
        expected_df = self.conn.execute("""
SELECT DISTINCT t.account_id,
       FIRST_VALUE(t.amount) OVER (
           PARTITION BY t.account_id ORDER BY t.txn_date, t.txn_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS first_amount,
       LAST_VALUE(t.amount) OVER (
           PARTITION BY t.account_id ORDER BY t.txn_date, t.txn_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_amount
FROM transactions t
WHERE t.status = 'completed'
ORDER BY t.account_id
        """).df()
        return check(solution, expected_df, self.conn, problem="P5: First and Last Completed Transaction per Account", ordered=True)

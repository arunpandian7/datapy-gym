import pyspark.sql.functions as F
from pyspark.sql import Window
from utils import check

GAP_DAYS = 30
SCD_CUSTOMER_IDS = list(range(1, 11))
UPDATES_DATA = [
    (1, "silver"), (2, "gold"), (3, "gold"), (4, "bronze"), (5, "platinum"),
    (6, "bronze"), (7, "silver"), (8, "silver"), (9, "bronze"), (10, "gold"),
]
CHANGE_DATE = "2024-01-15"


class Checker:
    def __init__(self, spark, customers, products, orders, order_items):
        self.spark = spark
        self.customers = customers.withColumn("signup_date", F.to_date("signup_date"))
        self.products = products
        self.orders = orders.withColumn("order_date", F.to_date("order_date"))
        self.order_items = order_items

    def _baseline(self):
        return (
            self.customers
            .filter(F.col("customer_id").isin(SCD_CUSTOMER_IDS))
            .select("customer_id", "tier", "signup_date")
        )

    def _updates_feed(self):
        return (
            self.spark.createDataFrame(UPDATES_DATA, ["customer_id", "new_tier"])
            .withColumn("change_date", F.lit(CHANGE_DATE).cast("date"))
        )

    def _changes(self):
        return (
            self._updates_feed().join(self._baseline(), "customer_id")
            .filter(F.col("new_tier") != F.col("tier"))
            .select("customer_id", F.col("tier").alias("old_tier"), "new_tier", "change_date")
        )

    def p1(self, solution):
        w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
        expected = (
            self.orders
            .withColumn("prev_date", F.lag("order_date").over(w))
            .withColumn("gap_days", F.datediff("order_date", "prev_date"))
            .withColumn(
                "is_new_session",
                F.when((F.col("prev_date").isNull()) | (F.col("gap_days") > GAP_DAYS), 1).otherwise(0),
            )
            .withColumn("session_id", F.sum("is_new_session").over(w))
            .select("customer_id", "order_id", "order_date", "session_id")
            .orderBy("customer_id", "order_date", "order_id")
        )
        return check(solution, expected,
                     problem="P1: Gap-Based Sessionization — Assign Session IDs",
                     ordered=True)

    def p2(self, solution):
        w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
        sessioned = (
            self.orders
            .withColumn("prev_date", F.lag("order_date").over(w))
            .withColumn("gap_days", F.datediff("order_date", "prev_date"))
            .withColumn(
                "is_new_session",
                F.when((F.col("prev_date").isNull()) | (F.col("gap_days") > GAP_DAYS), 1).otherwise(0),
            )
            .withColumn("session_id", F.sum("is_new_session").over(w))
        )
        expected = (
            sessioned
            .groupBy("customer_id", "session_id")
            .agg(
                F.min("order_date").alias("session_start"),
                F.max("order_date").alias("session_end"),
                F.count("order_id").alias("order_count"),
                F.round(F.sum("total_amount"), 2).alias("session_revenue"),
            )
            .orderBy("customer_id", "session_id")
        )
        return check(solution, expected,
                     problem="P2: Per-Session Summary — Start, End, Count, Revenue",
                     ordered=True)

    def p3(self, solution):
        expected = self._changes().orderBy("customer_id")
        return check(solution, expected,
                     problem="P3: Detect Changed Customers (CDC Diff)",
                     ordered=True)

    def p4(self, solution):
        baseline = self._baseline()
        changes = self._changes()

        expired = (
            baseline.join(changes.select("customer_id", "change_date"), "customer_id")
            .select(
                "customer_id", "tier",
                F.col("signup_date").alias("effective_start"),
                F.date_sub(F.col("change_date"), 1).alias("effective_end"),
                F.lit(False).alias("is_current"),
            )
        )
        new_current = (
            changes.select(
                "customer_id",
                F.col("new_tier").alias("tier"),
                F.col("change_date").alias("effective_start"),
                F.lit(None).cast("date").alias("effective_end"),
                F.lit(True).alias("is_current"),
            )
        )
        unchanged = (
            baseline.join(changes.select("customer_id"), "customer_id", "left_anti")
            .select(
                "customer_id", "tier",
                F.col("signup_date").alias("effective_start"),
                F.lit(None).cast("date").alias("effective_end"),
                F.lit(True).alias("is_current"),
            )
        )
        expected = (
            expired.unionByName(new_current).unionByName(unchanged)
            .orderBy("customer_id", "effective_start")
        )
        return check(solution, expected,
                     problem="P4: Build the SCD Type 2 Dimension",
                     ordered=True)

    def p5(self, solution):
        scd2 = self.p4_table()
        asof_dates = self.spark.createDataFrame(
            [(1, "2024-01-10"), (1, "2024-02-01"), (5, "2024-01-10"), (5, "2024-02-01"), (4, "2024-02-01")],
            ["customer_id", "as_of_date"],
        ).withColumn("as_of_date", F.col("as_of_date").cast("date"))

        expected = (
            asof_dates.join(scd2, "customer_id")
            .filter(
                (F.col("as_of_date") >= F.col("effective_start"))
                & (F.col("effective_end").isNull() | (F.col("as_of_date") <= F.col("effective_end")))
            )
            .select("customer_id", "as_of_date", F.col("tier").alias("tier_as_of"))
            .orderBy("customer_id", "as_of_date")
        )
        return check(solution, expected,
                     problem="P5: Point-in-Time Lookup Against the SCD2 Table",
                     ordered=True)

    def p4_table(self):
        """Recompute the P4 expected table for use as P5's input dimension."""
        baseline = self._baseline()
        changes = self._changes()
        expired = (
            baseline.join(changes.select("customer_id", "change_date"), "customer_id")
            .select(
                "customer_id", "tier",
                F.col("signup_date").alias("effective_start"),
                F.date_sub(F.col("change_date"), 1).alias("effective_end"),
                F.lit(False).alias("is_current"),
            )
        )
        new_current = (
            changes.select(
                "customer_id",
                F.col("new_tier").alias("tier"),
                F.col("change_date").alias("effective_start"),
                F.lit(None).cast("date").alias("effective_end"),
                F.lit(True).alias("is_current"),
            )
        )
        unchanged = (
            baseline.join(changes.select("customer_id"), "customer_id", "left_anti")
            .select(
                "customer_id", "tier",
                F.col("signup_date").alias("effective_start"),
                F.lit(None).cast("date").alias("effective_end"),
                F.lit(True).alias("is_current"),
            )
        )
        return expired.unionByName(new_current).unionByName(unchanged)

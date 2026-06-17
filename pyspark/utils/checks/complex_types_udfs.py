import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from utils import check


class Checker:
    def __init__(self, spark, customers, products, orders, order_items):
        self.spark = spark
        self.customers = customers
        self.products = products
        self.orders = orders
        self.order_items = order_items

    def _amounts(self):
        return (
            self.orders
            .groupBy("customer_id")
            .agg(F.array_sort(F.collect_list("total_amount")).alias("amounts"))
        )

    def p1(self, solution):
        expected = self._amounts().orderBy("customer_id")
        return check(solution, expected,
                     problem="P1: Build a Per-Customer Order Amount Array",
                     ordered=True)

    def p2(self, solution):
        expected = (
            self._amounts()
            .select(
                "customer_id",
                F.size("amounts").alias("order_count"),
                F.size(F.filter("amounts", lambda x: x > 100)).alias("large_count"),
                F.round(F.aggregate("amounts", F.lit(0.0), lambda acc, x: acc + x), 2)
                 .alias("total_amount_sum"),
            )
            .orderBy("customer_id")
        )
        return check(solution, expected,
                     problem="P2: Higher-Order Functions — filter, aggregate, size",
                     ordered=True)

    def p3(self, solution):
        expected = (
            self._amounts()
            .select(
                "customer_id",
                F.exists("amounts", lambda x: x > 4000).alias("has_large_order"),
                F.forall("amounts", lambda x: x > 500).alias("all_above_500"),
            )
            .orderBy("customer_id")
        )
        return check(solution, expected,
                     problem="P3: exists() and forall() — Per-Customer Flags",
                     ordered=True)

    def p4(self, solution):
        expected = (
            self.orders
            .groupBy("customer_id")
            .agg(F.array_max(F.collect_list(F.struct("total_amount", "order_id"))).alias("biggest"))
            .select(
                "customer_id",
                F.col("biggest.order_id").alias("biggest_order_id"),
                F.col("biggest.total_amount").alias("biggest_amount"),
            )
            .orderBy("customer_id")
        )
        return check(solution, expected,
                     problem="P4: Struct Ordering — Biggest Order per Customer",
                     ordered=True)

    def p5(self, solution):
        def tier_label(amount: float) -> str:
            if amount >= 4000:
                return "gold"
            elif amount >= 1000:
                return "silver"
            else:
                return "bronze"

        python_udf = F.udf(tier_label, StringType())
        expected = (
            self.orders
            .withColumn("loyalty_tier", python_udf(F.col("total_amount")))
            .select("order_id", "customer_id", "total_amount", "loyalty_tier")
            .orderBy("order_id")
        )
        return check(solution, expected,
                     problem="P5: Python UDF vs Pandas UDF — Loyalty Tier",
                     ordered=True)

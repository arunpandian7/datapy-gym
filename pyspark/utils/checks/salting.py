import pyspark.sql.functions as F
from utils import check


class Checker:
    def __init__(self, spark, customers, products, orders, order_items):
        self.spark = spark
        self.customers = customers
        self.products = products
        self.orders = orders
        self.order_items = order_items

    def p1(self, solution):
        expected = (
            self.orders
            .groupBy("customer_id")
            .agg(F.count("order_id").alias("order_count"))
            .orderBy(F.col("order_count").desc())
            .limit(5)
        )
        return check(solution, expected, problem="P1: Identify the Skew", ordered=True)

    def p2(self, solution):
        per_customer = (
            self.orders
            .groupBy("customer_id")
            .agg(F.count("order_id").alias("order_count"))
        )
        expected = per_customer.agg(
            F.max("order_count").alias("max_orders"),
            F.round(F.avg("order_count"), 2).alias("avg_orders"),
            F.round(F.max("order_count") / F.avg("order_count"), 2).alias("skew_ratio"),
        )
        return check(solution, expected, problem="P2: Quantify the Skew Ratio")

    def p3(self, solution):
        expected = (
            self.orders
            .groupBy("customer_id")
            .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue"))
            .orderBy("customer_id")
        )
        return check(solution, expected,
                     problem="P3: Salted GroupBy — Total Revenue per Customer",
                     ordered=True, precision=0.02)

    def p4(self, solution):
        lookup = (
            self.customers
            .filter(F.col("tier") == "platinum")
            .select("customer_id", "name")
        )
        expected = (
            self.orders.join(lookup, "customer_id")
            .select("order_id", "customer_id", "name", "total_amount")
            .orderBy("order_id")
        )
        return check(solution, expected,
                     problem="P4: Salted Join — Orders for Platinum Customers",
                     ordered=True, precision=0.01)

    def p5(self, solution):
        def _partition_stats(df, label):
            counts = (
                df
                .withColumn("_pid", F.spark_partition_id())
                .groupBy("_pid")
                .agg(F.count("*").alias("_rows"))
            )
            return counts.agg(
                F.lit(label).alias("approach"),
                F.max("_rows").alias("max_partition_rows"),
                F.round(F.avg("_rows"), 0).cast("long").alias("avg_partition_rows"),
                F.round(F.max("_rows") / F.avg("_rows"), 2).alias("hottest_ratio"),
            )

        unsalted = self.orders.repartition(10, F.col("customer_id"))
        salted_orders = (
            self.orders
            .withColumn("_salt", (F.rand(seed=42) * 10).cast("int"))
            .withColumn("salted_key", F.concat(
                F.col("customer_id").cast("string"),
                F.lit("_"),
                F.col("_salt").cast("string"),
            ))
        )
        salted = salted_orders.repartition(10, F.col("salted_key"))
        expected = (
            _partition_stats(unsalted, "unsalted")
            .union(_partition_stats(salted, "salted"))
            .orderBy("approach")
        )
        return check(solution, expected,
                     problem="P5: Compare Partition Hotspot Before and After Salting",
                     ordered=True)

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
            self.orders.repartition(4)
            .withColumn("partition_id", F.spark_partition_id())
            .groupBy("partition_id")
            .agg(F.count("*").alias("row_count"))
            .orderBy("partition_id")
        )
        return check(
            solution, expected, problem="P1: Row Distribution After repartition(4)", ordered=True
        )

    def p2(self, solution):
        expected = (
            self.orders.repartition(10, F.col("customer_id"))
            .withColumn("partition_id", F.spark_partition_id())
            .groupBy("partition_id")
            .agg(F.count("*").alias("row_count"))
            .orderBy(F.col("row_count").desc())
        )
        return check(solution, expected, problem="P2: Partition Skew on customer_id", ordered=True)

    def p3(self, solution):
        expected = (
            self.orders.repartition(20)
            .coalesce(5)
            .withColumn("partition_id", F.spark_partition_id())
            .groupBy("partition_id")
            .agg(F.count("*").alias("row_count"))
            .orderBy("partition_id")
        )
        return check(solution, expected, problem="P3: Coalesce vs Repartition", ordered=True)

    def p4(self, solution):
        self.spark.conf.set("spark.sql.adaptive.enabled", False)
        self.spark.conf.set("spark.sql.shuffle.partitions", "4")
        expected = (
            self.orders.groupBy("status")
            .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue"))
            .orderBy(F.col("total_revenue").desc())
        )
        self.spark.conf.set("spark.sql.shuffle.partitions", "8")
        result = check(solution, expected, problem="P4: Shuffle Partition Control", ordered=True)
        if solution is not None:
            n = solution.rdd.getNumPartitions()
            print(f"partition count: {n}  (expected 4: {n == 4})")
        return result

    def p5(self, solution):
        expected = (
            self.orders.repartitionByRange(5, F.col("total_amount"))
            .withColumn("partition_id", F.spark_partition_id())
            .groupBy("partition_id")
            .agg(
                F.min("total_amount").alias("min_amount"),
                F.max("total_amount").alias("max_amount"),
                F.count("*").alias("row_count"),
            )
            .orderBy("partition_id")
        )
        return check(solution, expected, problem="P5: Range-Based Partitioning", ordered=True)

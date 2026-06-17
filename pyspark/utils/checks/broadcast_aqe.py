import io
from contextlib import redirect_stdout

import pyspark.sql.functions as F
from utils import check


def _explain_str(df) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


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
            .join(F.broadcast(self.customers), "customer_id")
            .select("order_id", "customer_id", "tier", "total_amount")
            .orderBy("order_id")
        )
        return check(solution, expected,
                     problem="P1: Broadcast Join — Order Revenue with Customer Tier",
                     ordered=True)

    def p2(self, solution):
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        no_hint = self.orders.join(self.customers, "customer_id")
        explicit = self.orders.join(F.broadcast(self.customers), "customer_id")

        rows = [
            ("explicit_broadcast", "BroadcastHashJoin" in _explain_str(explicit)),
            ("no_hint", "BroadcastHashJoin" in _explain_str(no_hint)),
        ]
        expected = self.spark.createDataFrame(rows, ["join_type", "uses_broadcast"]).orderBy("join_type")

        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
        return check(solution, expected,
                     problem="P2: Read the Physical Plan — Disabled Threshold",
                     ordered=True)

    def p3(self, solution):
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
        default_join = self.orders.join(self.customers, "customer_id")
        merge_join = self.orders.join(self.customers.hint("merge"), "customer_id")

        rows = [
            (
                "default",
                "BroadcastHashJoin" in _explain_str(default_join),
                "SortMergeJoin" in _explain_str(default_join),
            ),
            (
                "merge_hint",
                "BroadcastHashJoin" in _explain_str(merge_join),
                "SortMergeJoin" in _explain_str(merge_join),
            ),
        ]
        expected = self.spark.createDataFrame(
            rows, ["join_type", "uses_broadcast", "uses_sort_merge"]
        ).orderBy("join_type")

        return check(solution, expected,
                     problem="P3: Force a Sort-Merge Join with .hint(\"merge\")",
                     ordered=True)

    def p4(self, solution):
        joined = (
            self.order_items
            .join(F.broadcast(self.products), "product_id")
            .join(self.orders, "order_id")
            .join(F.broadcast(self.customers), "customer_id")
        )
        expected = (
            joined
            .groupBy("category", "tier")
            .agg(
                F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_revenue"),
                F.count("item_id").alias("item_count"),
            )
            .orderBy("category", "tier")
        )
        return check(solution, expected,
                     problem="P4: Multi-Way Broadcast Join — Revenue by Category and Tier",
                     ordered=True)

    def p5(self, solution):
        self.spark.conf.set("spark.sql.shuffle.partitions", "200")

        self.spark.conf.set("spark.sql.adaptive.enabled", "false")
        no_aqe = self.order_items.groupBy("product_id").agg(F.sum("quantity").alias("q"))
        no_aqe.count()
        no_aqe_partitions = no_aqe.rdd.getNumPartitions()

        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        aqe = self.order_items.groupBy("product_id").agg(F.sum("quantity").alias("q"))
        aqe.count()
        aqe_partitions = aqe.rdd.getNumPartitions()

        rows = [
            ("aqe_coalesced", aqe_partitions),
            ("no_aqe", no_aqe_partitions),
        ]
        expected = self.spark.createDataFrame(rows, ["scenario", "num_partitions"]).orderBy("scenario")

        self.spark.conf.set("spark.sql.shuffle.partitions", "8")
        return check(solution, expected,
                     problem="P5: AQE Coalesce Partitions — Shrinking Tiny Shuffle Output",
                     ordered=True)

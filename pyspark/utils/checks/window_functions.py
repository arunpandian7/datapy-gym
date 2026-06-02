import pyspark.sql.functions as F
from pyspark.sql import Window
from utils import check


class Checker:
    def __init__(self, spark, customers, products, orders, order_items):
        self.spark = spark
        self.customers = customers
        self.products = products
        self.orders = orders
        self.order_items = order_items

    def p1(self, solution):
        agg = (
            self.order_items.join(self.products, "product_id")
            .groupBy("product_id", F.col("name"), "category")
            .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue"))
        )
        w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
        expected = (
            agg
            .withColumn("rank", F.dense_rank().over(w))
            .filter(F.col("rank") <= 3)
            .select("category", "name", "revenue", "rank")
            .orderBy("category", "rank")
        )
        return check(solution, expected,
                     problem="P1: Top 3 Products by Revenue Within Each Category", ordered=True)

    def p2(self, solution):
        daily = (
            self.orders
            .groupBy("order_date")
            .agg(F.round(F.sum("total_amount"), 2).alias("daily_revenue"))
        )
        w = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        expected = (
            daily
            .withColumn("running_total", F.round(F.sum("daily_revenue").over(w), 2))
            .orderBy("order_date")
        )
        return check(solution, expected, problem="P2: Cumulative Daily Revenue", ordered=True)

    def p3(self, solution):
        monthly = (
            self.orders
            .withColumn("month", F.date_format("order_date", "yyyy-MM"))
            .groupBy("month")
            .agg(F.round(F.sum("total_amount"), 2).alias("monthly_revenue"))
        )
        w = Window.orderBy("month")
        expected = (
            monthly
            .withColumn("prev_revenue", F.lag("monthly_revenue", 1).over(w))
            .withColumn(
                "mom_change_pct",
                F.round(
                    (F.col("monthly_revenue") - F.col("prev_revenue"))
                    / F.col("prev_revenue") * 100,
                    2,
                ),
            )
            .orderBy("month")
        )
        return check(solution, expected, problem="P3: Month-over-Month Revenue Change", ordered=True)

    def p4(self, solution):
        agg = (
            self.orders.filter(F.col("status") == "completed")
            .join(self.customers, "customer_id")
            .groupBy("customer_id", "name", "tier")
            .agg(F.round(F.sum("total_amount"), 2).alias("total_spend"))
        )
        w = Window.partitionBy("tier").orderBy(F.col("total_spend").desc())
        expected = (
            agg
            .withColumn("rank_in_tier", F.row_number().over(w))
            .filter(F.col("rank_in_tier") <= 2)
            .select("tier", "customer_id", "name", "total_spend", "rank_in_tier")
            .orderBy("tier", "rank_in_tier")
        )
        return check(solution, expected,
                     problem="P4: Top 2 Customers per Tier by Spend (Completed Orders Only)",
                     ordered=True)

    def p5(self, solution):
        w = Window.partitionBy("status").orderBy("total_amount")
        expected = (
            self.orders
            .withColumn("pct_rank", F.round(F.percent_rank().over(w), 4))
            .select("order_id", "status", "total_amount", "pct_rank")
            .orderBy("status", "total_amount")
        )
        return check(solution, expected,
                     problem="P5: Percentile Rank of Order Amounts Within Each Status",
                     ordered=True)

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
            self.order_items.join(self.products, "product_id")
            .groupBy("category")
            .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_revenue"))
            .orderBy(F.col("total_revenue").desc())
        )
        return check(solution, expected, problem="P1: Revenue by Product Category")

    def p2(self, solution):
        expected = (
            self.orders.filter(F.col("status") == "completed")
            .join(self.customers, "customer_id")
            .groupBy("customer_id", "name")
            .agg(F.round(F.sum("total_amount"), 2).alias("total_spend"))
            .orderBy(F.col("total_spend").desc())
            .limit(5)
        )
        return check(solution, expected, problem="P2: Top 5 Customers by Completed Spend", ordered=True)

    def p3(self, solution):
        expected = (
            self.orders
            .withColumn("month", F.date_format("order_date", "yyyy-MM"))
            .groupBy("month")
            .agg(
                F.count("order_id").alias("order_count"),
                F.round(F.sum("total_amount"), 2).alias("monthly_revenue"),
            )
            .orderBy("month")
        )
        return check(solution, expected, problem="P3: Monthly Revenue Trend", ordered=True)

    def p4(self, solution):
        expected = (
            self.order_items.join(self.products, "product_id")
            .groupBy("category")
            .agg(F.round(F.avg("unit_price"), 2).alias("avg_item_price"))
            .filter(F.col("avg_item_price") > 100)
            .orderBy(F.col("avg_item_price").desc())
        )
        return check(solution, expected, problem="P4: Categories Where Average Item Price Exceeds $100")

    def p5(self, solution):
        expected = (
            self.orders.join(self.customers, "customer_id")
            .groupBy("tier")
            .agg(
                F.countDistinct("customer_id").alias("unique_customers"),
                F.count("order_id").alias("total_orders"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            )
            .withColumn("revenue_per_customer",
                        F.round(F.col("total_revenue") / F.col("unique_customers"), 2))
            .orderBy("tier")
        )
        return check(solution, expected, problem="P5: Customer Tier Performance Summary")

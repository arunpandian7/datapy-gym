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
        products_r = self.products.withColumnRenamed("name", "product_name")
        customers_r = self.customers.withColumnRenamed("name", "customer_name")
        expected = (
            self.order_items
            .join(self.orders, "order_id")
            .filter(F.col("status") == "completed")
            .join(products_r, "product_id")
            .join(customers_r, "customer_id")
            .select(
                "order_id", "order_date", "customer_name", "tier",
                "product_name", "category", "quantity", "unit_price",
                F.round(F.col("quantity") * F.col("unit_price"), 2).alias("line_total"),
            )
        )
        return check(solution, expected, problem="P1: Enriched Order Line Items", ordered=False)

    def p2(self, solution):
        ordered_customers = self.orders.select("customer_id").distinct()
        expected = (
            self.customers
            .join(ordered_customers, on="customer_id", how="left_anti")
            .select("customer_id", "name", "email", "tier")
            .orderBy("customer_id")
        )
        return check(solution, expected, problem="P2: Customers Who Have Never Ordered", ordered=True)

    def p3(self, solution):
        a = self.order_items.alias("a")
        b = self.order_items.alias("b")
        expected = (
            a.join(b, on="order_id")
            .filter(F.col("a.product_id") < F.col("b.product_id"))
            .groupBy(
                F.col("a.product_id").alias("product_id_1"),
                F.col("b.product_id").alias("product_id_2"),
            )
            .agg(F.count("*").alias("times_bought_together"))
            .orderBy(F.col("times_bought_together").desc())
            .limit(10)
        )
        return check(solution, expected,
                     problem="P3: Most Frequently Bought Product Pairs", ordered=True)

    def p4(self, solution):
        items_with_cat = (
            self.order_items
            .join(self.products, "product_id")
            .join(self.orders.select("order_id", "customer_id"), "order_id")
        )
        elec = items_with_cat.filter(F.col("category") == "Electronics").select("customer_id").distinct()
        sport = items_with_cat.filter(F.col("category") == "Sports").select("customer_id").distinct()
        expected = (
            elec.join(sport, "customer_id")
            .join(self.customers.select("customer_id", "name"), "customer_id")
            .orderBy("customer_id")
        )
        return check(solution, expected,
                     problem="P4: Customers Who Bought From Both Electronics AND Sports",
                     ordered=True)

    def p5(self, solution):
        total = self.orders.agg(F.sum("total_amount")).first()[0]
        expected = (
            self.orders.join(self.customers, "customer_id")
            .groupBy("tier")
            .agg(F.round(F.sum("total_amount"), 2).alias("tier_revenue"))
            .withColumn("revenue_share_pct",
                        F.round(F.col("tier_revenue") / total * 100, 2))
            .orderBy("tier")
        )
        return check(solution, expected, problem="P5: Revenue Share by Customer Tier", ordered=True)

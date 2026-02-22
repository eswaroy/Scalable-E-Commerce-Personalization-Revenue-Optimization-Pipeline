from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():

    spark = SparkSession.builder \
        .appName("EcommerceAnalytics") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("USE ecommerce")

    # -------------------------------------------------
    #  High-Value Customers
    # -------------------------------------------------
    print("\nTop 20 High-Activity Customers:")

    high_value = spark.sql("""
        SELECT user_id,
               COUNT(*) AS total_reviews,
               AVG(rating) AS avg_rating
        FROM fact_reviews
        GROUP BY user_id
        ORDER BY total_reviews DESC
        LIMIT 20
    """)

    high_value.show()

    # -------------------------------------------------
    #  Product Popularity Distribution
    # -------------------------------------------------
    print("\nTop 20 Most Reviewed Products:")

    popular_products = spark.sql("""
        SELECT parent_asin,
               COUNT(*) AS review_count
        FROM fact_reviews
        GROUP BY parent_asin
        ORDER BY review_count DESC
        LIMIT 20
    """)

    popular_products.show()

    # -------------------------------------------------
    # 3 Verified vs Non-Verified Purchase Behavior
    # -------------------------------------------------
    print("\nVerified Purchase Behavior:")

    verified_stats = spark.sql("""
        SELECT verified_purchase,
               COUNT(*) AS total_reviews,
               AVG(rating) AS avg_rating
        FROM fact_reviews
        GROUP BY verified_purchase
    """)

    verified_stats.show()

    # -------------------------------------------------
    #  Revenue Proxy (Price * Review Count)
    # -------------------------------------------------
    print("\nRevenue Proxy by Product (Top 20):")

    revenue_proxy = spark.sql("""
        SELECT p.parent_asin,
               p.product_title,
               p.price,
               COUNT(f.parent_asin) AS review_count,
               (COUNT(f.parent_asin) * p.price) AS revenue_proxy
        FROM fact_reviews f
        JOIN dim_products p
        ON f.parent_asin = p.parent_asin
        WHERE p.price IS NOT NULL
        GROUP BY p.parent_asin, p.product_title, p.price
        ORDER BY revenue_proxy DESC
        LIMIT 20
    """)

    revenue_proxy.show(truncate=False)

    # -------------------------------------------------
    # Seasonal Demand Trend
    # -------------------------------------------------
    print("\nMonthly Demand Trend:")

    seasonal_trend = spark.sql("""
        SELECT year,
               month,
               COUNT(*) AS total_reviews
        FROM fact_reviews
        GROUP BY year, month
        ORDER BY year, month
    """)

    seasonal_trend.show(50)

    spark.stop()


if __name__ == "__main__":
    main()
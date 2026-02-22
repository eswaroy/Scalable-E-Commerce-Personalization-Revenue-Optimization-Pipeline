
from pyspark.sql import SparkSession

def main():

    # -------------------------------
    #  Create Spark Session
    # -------------------------------
    spark = SparkSession.builder \
        .appName("EcommerceWarehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("Spark Session Started")

    # -------------------------------
    # Create & Use Database
    # -------------------------------
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")
    spark.sql("USE ecommerce")

    # -------------------------------
    # Create fact_reviews Table
    # -------------------------------
    spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS fact_reviews (
        user_id STRING,
        product_id STRING,
        parent_asin STRING,
        rating DOUBLE,
        review_text STRING,
        review_ts BIGINT,
        verified_purchase BOOLEAN,
        review_date TIMESTAMP
    )
    PARTITIONED BY (category STRING, year INT, month INT)
    STORED AS PARQUET
    LOCATION 'hdfs:///beauty/processed/reviews_parquet'
    """)

    spark.sql("MSCK REPAIR TABLE fact_reviews")

    # -------------------------------
    #  Create dim_products Table
    # -------------------------------
    spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS dim_products (
        parent_asin STRING,
        product_title STRING,
        main_category STRING,
        average_rating DOUBLE,
        rating_number INT,
        price DOUBLE,
        brand STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs:///beauty/processed/meta_parquet'
    """)

    # -------------------------------
    # Validate Tables
    # -------------------------------
    print("Tables in ecommerce database:")
    spark.sql("SHOW TABLES").show()

    print("Fact Reviews Count:")
    spark.sql("SELECT COUNT(*) FROM fact_reviews").show()

    print("Product Dimension Count:")
    spark.sql("SELECT COUNT(*) FROM dim_products").show()

    # -------------------------------
    #  Sample Join Validation
    # -------------------------------
    print("Sample Joined Data:")
    spark.sql("""
        SELECT f.user_id,
               p.product_title,
               f.rating
        FROM fact_reviews f
        LEFT JOIN dim_products p
        ON f.parent_asin = p.parent_asin
        LIMIT 10
    """).show(truncate=False)

    spark.stop()
    print("Spark Session Stopped")


if __name__ == "__main__":
    main()
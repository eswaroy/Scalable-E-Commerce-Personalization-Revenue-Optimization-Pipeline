from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS


def main():

    # -------------------------------------------------
    #  Spark Session
    # -------------------------------------------------
    spark = SparkSession.builder \
        .appName("Ecommerce_FinalSystem") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE ecommerce")

    print("\nLoading interactions...")

    df = spark.sql("""
        SELECT user_id, parent_asin, rating
        FROM fact_reviews
        WHERE rating IS NOT NULL
    """)

    # -------------------------------------------------
    #  Filter Sparse Users & Items
    # -------------------------------------------------
    active_users = df.groupBy("user_id") \
                     .count() \
                     .filter("count >= 5") \
                     .select("user_id")

    df = df.join(active_users, "user_id")

    popular_items = df.groupBy("parent_asin") \
                      .count() \
                      .filter("count >= 10") \
                      .select("parent_asin")

    df = df.join(popular_items, "parent_asin")

    print("Filtered interactions:", df.count())

    # -------------------------------------------------
    #  Convert to Implicit Feedback
    # -------------------------------------------------
    df = df.withColumn(
        "rating",
        F.when(F.col("rating") >= 4, 1.0).otherwise(0.0)
    )

    df = df.repartition(8)

    # Hash indexing
    df = df.withColumn(
        "user_index",
        (F.abs(F.hash("user_id")) % 5000000).cast("int")
    )

    df = df.withColumn(
        "item_index",
        (F.abs(F.hash("parent_asin")) % 5000000).cast("int")
    )

    train, test = df.randomSplit([0.8, 0.2], seed=42)

    print("Train size:", train.count())
    print("Test size:", test.count())

    # -------------------------------------------------
    #  Popularity Baseline
    # -------------------------------------------------
    print("\nBuilding popularity baseline...")

    popularity = train.groupBy("item_index") \
                      .count() \
                      .orderBy(F.desc("count"))

    top10_popular = popularity.limit(10).select("item_index")

    # Assign same top10 to all test users
    test_users = test.select("user_index").distinct()

    pop_recs = test_users.crossJoin(top10_popular)

    # -------------------------------------------------
    # Train Implicit ALS
    # -------------------------------------------------
    print("\nTraining Implicit ALS...")

    als = ALS(
        userCol="user_index",
        itemCol="item_index",
        ratingCol="rating",
        implicitPrefs=True,
        alpha=40,
        coldStartStrategy="drop",
        rank=15,
        maxIter=8,
        regParam=0.1
    )

    model = als.fit(train)

    print("Generating ALS recommendations...")

    als_recs = model.recommendForUserSubset(test_users, 10)

    als_exploded = als_recs.select(
        "user_index",
        F.explode("recommendations").alias("rec")
    ).select(
        "user_index",
        F.col("rec.item_index").alias("item_index")
    )

    # -------------------------------------------------
    #  Define Relevant Items
    # -------------------------------------------------
    test_relevant = test.filter("rating == 1.0") \
                        .select("user_index", "item_index") \
                        .distinct()

    # -------------------------------------------------
    #  Precision@10 Calculation
    # -------------------------------------------------
    def compute_precision(predictions, name):

        hits = predictions.join(
            test_relevant,
            ["user_index", "item_index"],
            "inner"
        )

        total_hits = hits.count()
        num_users = test_users.count()

        precision = total_hits / (num_users * 10)

        print(f"\n{name} Precision@10: {precision}")

        return precision


    pop_precision = compute_precision(pop_recs, "Popularity")
    als_precision = compute_precision(als_exploded, "ALS")

    # -------------------------------------------------
    #  Revenue Uplift Simulation
    # -------------------------------------------------
    print("\nSimulating revenue uplift...")

    baseline_ctr = pop_precision
    personalized_ctr = als_precision

    avg_order_value = 40  # proxy assumption
    impressions = 1_000_000
    conversion_rate = 0.05

    baseline_revenue = impressions * baseline_ctr * conversion_rate * avg_order_value
    personalized_revenue = impressions * personalized_ctr * conversion_rate * avg_order_value

    uplift_percent = ((personalized_revenue - baseline_revenue) / baseline_revenue) * 100 if baseline_revenue != 0 else 0

    print("\nRevenue Simulation Results:")
    print("Baseline Revenue:", baseline_revenue)
    print("Personalized Revenue:", personalized_revenue)
    print("Revenue Uplift %:", uplift_percent)

    spark.stop()


if __name__ == "__main__":
    main()
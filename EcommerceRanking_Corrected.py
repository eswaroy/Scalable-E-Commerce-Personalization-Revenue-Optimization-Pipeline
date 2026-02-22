from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS


def main():

    # -------------------------------------------------
    #  Spark Session
    # -------------------------------------------------
    spark = SparkSession.builder \
        .appName("EcommerceRanking_Corrected") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE ecommerce")

    print("\nLoading data...")

    df = spark.sql("""
        SELECT user_id, parent_asin, rating
        FROM fact_reviews
        WHERE rating IS NOT NULL
    """)

    print("Original interactions:", df.count())

    # -------------------------------------------------
    # Filter Sparse Users (>=5 interactions)
    # -------------------------------------------------
    active_users = df.groupBy("user_id") \
                     .count() \
                     .filter("count >= 5") \
                     .select("user_id")

    df = df.join(active_users, "user_id")

    print("After user filtering:", df.count())

    # -------------------------------------------------
    #  Filter Sparse Items (>=10 interactions)
    # -------------------------------------------------
    popular_items = df.groupBy("parent_asin") \
                      .count() \
                      .filter("count >= 10") \
                      .select("parent_asin")

    df = df.join(popular_items, "parent_asin")

    print("After item filtering:", df.count())

    # -------------------------------------------------
    # Repartition
    # -------------------------------------------------
    df = df.repartition(8)

    # -------------------------------------------------
    #  Hash-based Indexing
    # -------------------------------------------------
    df = df.withColumn(
        "user_index",
        (F.abs(F.hash("user_id")) % 5000000).cast("int")
    )

    df = df.withColumn(
        "item_index",
        (F.abs(F.hash("parent_asin")) % 5000000).cast("int")
    )

    # -------------------------------------------------
    #  Train/Test Split
    # -------------------------------------------------
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    print("Train size:", train.count())
    print("Test size:", test.count())

    # -------------------------------------------------
    #  Train ALS
    # -------------------------------------------------
    print("\nTraining ALS model...")

    als = ALS(
        userCol="user_index",
        itemCol="item_index",
        ratingCol="rating",
        coldStartStrategy="drop",
        rank=10,
        maxIter=5,
        regParam=0.15,
        implicitPrefs=False
    )

    model = als.fit(train)

    print("Model trained.")

    # -------------------------------------------------
    #  Generate Recommendations ONLY for Test Users
    # -------------------------------------------------
    print("\nGenerating Top-10 recommendations for test users...")

    test_users = test.select("user_index").distinct()

    user_recs = model.recommendForUserSubset(test_users, 10)

    # Explode recommendations
    exploded_recs = user_recs.select(
        "user_index",
        F.explode("recommendations").alias("rec")
    ).select(
        "user_index",
        F.col("rec.item_index").alias("item_index")
    )

    # -------------------------------------------------
    #  Define Relevant Items (rating >= 4)
    # -------------------------------------------------
    test_relevant = test.filter("rating >= 4") \
                        .select("user_index", "item_index") \
                        .distinct()

    # -------------------------------------------------
    # Compute Hits
    # -------------------------------------------------
    hits = exploded_recs.join(
        test_relevant,
        on=["user_index", "item_index"],
        how="inner"
    )

    total_hits = hits.count()
    num_test_users = test_users.count()

    precision_at_10 = total_hits / (num_test_users * 10)

    print("\nEvaluation Results:")
    print("Total test users:", num_test_users)
    print("Total hits:", total_hits)
    print(f"Precision@10: {precision_at_10}")

    spark.stop()


if __name__ == "__main__":
    main()
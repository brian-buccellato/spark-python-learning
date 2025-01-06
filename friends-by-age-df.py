from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

friends_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("fakefriends-header.csv")
)

friends_df.select("age", "friends").groupBy("age").avg("friends").withColumnRenamed(
    "avg(friends)", "avg_friends"
).sort("age").show()

spark.stop()

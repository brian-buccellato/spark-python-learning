from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperheroDF").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

# Load up the superhero names
names = spark.read.schema(schema).option("sep", " ").csv("MarvelNames.txt")

lines = spark.read.text("MarvelGraph.txt")

connections = (
    lines.withColumn("id", F.split(F.col("value"), " ")[0])
    .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(F.sum("connections").alias("connections"))
)

most_popular = connections.sort("connections", ascending=False).first()

most_popular_name = names.filter(names.id == most_popular.id).select("name").first()

print(
    f"{most_popular_name.name} is the most popular superhero with {most_popular.connections} co-appearances."
)

spark.stop()

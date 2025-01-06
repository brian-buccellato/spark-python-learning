from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("LeastPopularSuperheroDF").getOrCreate()

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

#assuming the least is one co-appearance
least_popular = connections.filter(connections.connections == 1).sort("id")

least_popular_with_names = least_popular.join(names, least_popular.id == names.id)

print("The least popular superheroes with only 1 co-appearance are:")
least_popular_with_names.show()

# calculating the actual amount of co-appearances of the least popular superhero
min_conections = least_popular.agg(F.min("connections")).first()[0]
true_least_popular = connections.filter(connections.connections == min_conections).sort("id")
true_least_popular_with_names = true_least_popular.join(names, true_least_popular.id == names.id)

print("The least popular superheros with the least co-appearances is:")
true_least_popular_with_names.show()

spark.stop()
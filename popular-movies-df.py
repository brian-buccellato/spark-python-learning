from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMoviesDF").getOrCreate()

# Create schema when reading u.data
schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("movie_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# Load up movie data as dataframe
movies_df = spark.read.option("sep", "\t").schema(schema).csv("ml-100k/u.data")

top_movie_ids = (
    movies_df.groupBy("movie_id")
    .count()
    .orderBy(F.desc("count"))
)

top_movie_ids.show(10)

spark.stop()
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def load_movie_names():
    movie_names = {}
    with codecs.open("ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]
    return movie_names

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

name_dict = spark.sparkContext.broadcast(load_movie_names())

# Load up movie data as dataframe
movies_df = spark.read.option("sep", "\t").schema(schema).csv("ml-100k/u.data")

movie_counts = movies_df.groupBy("movie_id").count()

lookup_name = F.udf(lambda movie_id: name_dict.value[movie_id])

movies_with_names = movie_counts.withColumn("movie_title", lookup_name(F.col("movie_id")))

top_movie_ids = movies_with_names.orderBy(F.desc("count"))

top_movie_ids.show(10, False)

spark.stop()
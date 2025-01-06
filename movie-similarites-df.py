from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def computeJaccardSimilarity(spark, moviePairs, ratingsCounts):
    """
    Calculates the Jaccard similarity between pairs of movies based on user ratings.

    Args:
      spark (SparkSession): The Spark session.
      moviePairs (DataFrame): DataFrame containing pairs of movies.
      ratingsCounts (DataFrame): DataFrame containing ratings counts for movies.

    Returns:
      DataFrame: DataFrame containing the Jaccard similarity scores for movie pairs, sorted in descending order.
    """
    # Calculate the number of users who rated both movies
    pairCounts = moviePairs.groupBy("movie1", "movie2").agg(
        func.count(func.col("rating1")).alias("numPairs")
    )
    allCounts = (
        pairCounts.join(ratingsCounts, pairCounts.movie1 == ratingsCounts.movieId)
        .withColumnRenamed("ratingsCount", "movie1RatingCount")
        .select("movie1", "movie2", "numPairs", "movie1RatingCount")
        .join(ratingsCounts, pairCounts.movie2 == ratingsCounts.movieId)
        .withColumnRenamed("ratingsCount", "movie2RatingCount")
        .select(
            "movie1", "movie2", "numPairs", "movie1RatingCount", "movie2RatingCount"
        )
    )
    allCounts = allCounts.withColumn(
        "score",
        func.col("numPairs")
        / (
            func.col("movie1RatingCount")
            + func.col("movie2RatingCount")
            - func.col("numPairs")
        ),
    )
    return allCounts.sort(func.desc("score")).filter("movie1RatingCount > 10").filter("movie2RatingCount > 10").filter("numPairs > 1")

def computeCosineSimilarity(spark, data):
    """
    Compute the cosine similarity between movies based on their ratings.
    Args:
      spark (SparkSession): The Spark session.
      data (DataFrame): The input data containing movie ratings.
    Returns:
      DataFrame: A DataFrame containing the movie pairs, their similarity scores, and the number of pairs used for calculation.
    """
    # Compute xx, xy and yy columns
    pairScores = (
        data.withColumn("xx", func.col("rating1") * func.col("rating1"))
        .withColumn("yy", func.col("rating2") * func.col("rating2"))
        .withColumn("xy", func.col("rating1") * func.col("rating2"))
    )

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
        func.sum(func.col("xy")).alias("numerator"),
        (
            func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))
        ).alias("denominator"),
        func.count(func.col("xy")).alias("numPairs"),
    )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity.withColumn(
        "score",
        func.when(
            func.col("denominator") != 0,
            func.col("numerator") / func.col("denominator"),
        ).otherwise(0),
    ).select("movie1", "movie2", "score", "numPairs")

    return result


# Get movie name by given movie id
def getMovieName(movieNames, movieId):
    """
    Retrieves the name of a movie based on its ID.
    Parameters:
    - movieNames (DataFrame): DataFrame containing movie names and IDs.
    - movieId (int): ID of the movie to retrieve the name for.
    Returns:
    - str: The name of the movie.
    """
    result = (
        movieNames.filter(func.col("movieID") == movieId)
        .select("movieTitle")
        .collect()[0]
    )

    return result[0]


# Use every CPU core available on the machine "local[*]" ...do not use in production
spark = (
    SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
)

movieNamesSchema = StructType(
    [
        StructField("movieID", IntegerType(), True),
        StructField("movieTitle", StringType(), True),
    ]
)

moviesSchema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)


# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = (
    spark.read.option("sep", "|")
    .option("charset", "ISO-8859-1")
    .schema(movieNamesSchema)
    .csv("ml-100k/u.item")
)

# Load up movie data as dataset
movies = spark.read.option("sep", "\t").schema(moviesSchema).csv("ml-100k/u.data")


ratings = movies.select("userId", "movieId", "rating").filter("rating > 3")
# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = (
    ratings.alias("ratings1")
    .join(
        ratings.alias("ratings2"),
        (func.col("ratings1.userId") == func.col("ratings2.userId"))
        & (func.col("ratings1.movieId") < func.col("ratings2.movieId")),
    )
    .select(
        func.col("ratings1.movieId").alias("movie1"),
        func.col("ratings2.movieId").alias("movie2"),
        func.col("ratings1.rating").alias("rating1"),
        func.col("ratings2.rating").alias("rating2"),
    )
)
ratingsCounts = ratings.groupBy("movieId").agg(
    func.count(func.col("rating")).alias("ratingsCount")
)

moviePairSimilarities = computeJaccardSimilarity(spark, moviePairs, ratingsCounts).cache()
# moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if len(sys.argv) > 1:
    scoreThreshold = 0.30
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID))
        & (func.col("score") > scoreThreshold)
        & (func.col("numPairs") > coOccurrenceThreshold)
    )

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(
            getMovieName(movieNames, similarMovieID)
            + "\tscore: "
            + str(result.score)
            + "\tstrength: "
            + str(result.numPairs)
        )

spark.stop()

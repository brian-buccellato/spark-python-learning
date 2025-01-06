from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("WordCountDF").getOrCreate()

# Read the file as a dataframe
dataframe = spark.read.text("book.txt")

# here is the raw dataframe ugly as hell
dataframe.show()

# Split the lines into words
ugly_words = dataframe.select(F.split(dataframe.value, r"\W+").alias("word")).show()

# Explode the words into rows
words = dataframe.select(F.explode(F.split(dataframe.value, r"\W+")).alias("word"))
words.show()

# get rid of the empty strings
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(
    F.lower(wordsWithoutEmptyString.word).alias("word")
)
lowercaseWords.show()

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()
wordCounts.show()

# Sort by counts
wordCountsSorted = wordCounts.sort("count", ascending=False)
wordCountsSorted.show()

spark.stop()

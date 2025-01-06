from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

spark = SparkSession.builder.appName("MinMaxTempDF").getOrCreate()

# Define the schema
schema = StructType(
    [
        StructField("stationID", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

# Read the file as a dataframe
dataframe = spark.read.schema(schema).csv("1800.csv")

min_temps = dataframe.filter(dataframe.measure_type == "TMIN")
max_temps = dataframe.filter(dataframe.measure_type == "TMAX")

minStationTemps = min_temps.select("stationID", "temperature")
maxStationTemps = max_temps.select("stationID", "temperature")

minTemps = (
    minStationTemps.groupBy("stationID")
    .min("temperature")
    .withColumnRenamed("min(temperature)", "minTempF")
)
maxTemps = (
    maxStationTemps.groupBy("stationID")
    .max("temperature")
    .withColumnRenamed("max(temperature)", "maxTempF")
)

# minTemps.show()
# maxTemps.show()

# convert from tenths of Celsius to Fahrenheit
minTempsF = minTemps.withColumn(
    "minTempF", F.round(F.col("minTempF") * 0.1 * (9.0 / 5.0) + 32.0, 2)
)
maxTempsF = maxTemps.withColumn(
    "maxTempF", F.round(F.col("maxTempF") * 0.1 * (9.0 / 5.0) + 32.0, 2)
)

# minTempsF.show()
# maxTempsF.show()

# Join the two datasets
joined = minTempsF.join(maxTempsF, "stationID")
joined.show()

spark.stop()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

access_lines = spark.readStream.text("logs")

# Parse out the common log format to a DataFrame
content_size_exp = r'\s(\d+)$'
status_exp = r'\s(\d{3})\s'
general_exp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
time_exp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
host_exp = r'(^\S+\.[\S+\.]+\S+)\s'

logs_df = access_lines.select(regexp_extract('value', host_exp, 1).alias('host'),
                              regexp_extract('value', time_exp, 1).alias('timestamp'),
                              regexp_extract('value', general_exp, 1).alias('method'),
                              regexp_extract('value', general_exp, 2).alias('endpoint'),
                              regexp_extract('value', general_exp, 3).alias('protocol'),
                              regexp_extract('value', status_exp, 1).cast('integer').alias('status'),
                              )

# Keep a running count of status codes
status_counts_df = logs_df.groupBy(logs_df.status).count()

# Kick off our streaming query, dumping results to the console
query = (status_counts_df.writeStream.outputMode("complete").format("console").queryName("counts").start())

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
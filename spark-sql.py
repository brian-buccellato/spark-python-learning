from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode("utf-8")),
        age=int(fields[2]),
        numFriends=int(fields[3]),
    )

# lines creates an RDD because this is using spark context
lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# "people" comes from naming the temp view.  "age" was explicitly set in the mapper function
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()

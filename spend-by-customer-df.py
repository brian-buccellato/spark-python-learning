from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName("SpendByCustomerDF").getOrCreate()

schema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("amount_spent", FloatType(), True),
    ]
)
# Read the file as a dataframe
dataframe = spark.read.schema(schema).csv("customer-orders.csv")

# dataframe.show()

spendByCustomer = (
    dataframe.select("customer_id", "amount_spent")
    .groupBy("customer_id")
    .agg(F.round(F.sum("amount_spent"), 2).alias("total_spent"))
    .sort("total_spent", ascending=False)
)
spendByCustomer.show()

spark.stop()

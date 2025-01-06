"""
Spark ML Example to predict the price per unit area on house age, 
distance to public transit, and number of nearby convenience stores

- Use a decision tree regressor 
  (decision trees can handle data in different scales better than linear regression)
- Follow similar strategy as in linear-regression.py
- We have a header row so no schema is needed
"""

from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

# Load up our data as a df.
data = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv("realestate.csv")
    .select("HouseAge", "DistanceToMRT", "NumberConvenienceStores", "PriceOfUnitArea")
)

# Create a vector assembler for feature columns
assembler = VectorAssembler(
    inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"],
    outputCol="features",
)

feature_data = assembler.transform(data).select(
    "features", "PriceOfUnitArea")

train_data, test_data = feature_data.randomSplit([0.8, 0.2])


dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")
model = dtr.fit(train_data)
results = model.transform(test_data)

results.show()

# Evaluate the model
metrics = ["rmse", "mse", "r2", "mae"]
evaluator = RegressionEvaluator(labelCol="PriceOfUnitArea")
for metric in metrics:
    value = evaluator.evaluate(results, {evaluator.metricName: metric})
    print(f"{metric}: {value}")

# Stop the spark session
spark.stop()

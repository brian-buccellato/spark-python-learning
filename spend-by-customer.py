from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)


input = sc.textFile("customer-orders.csv")
rdd = input.map(parseLine)

totals_by_customer = rdd.reduceByKey(lambda x, y: x + y)
sorted_totals = totals_by_customer.map(lambda x: (x[1], x[0])).sortByKey().collect()


for result in sorted_totals:
    print(f"Customer: {result[1]} || Total Spent: {round(result[0], 2)}")

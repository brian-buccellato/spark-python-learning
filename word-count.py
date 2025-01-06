from pyspark import SparkConf, SparkContext
import re


def normalize_words(text):
    return re.compile(r"\W+", re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input_lines = sc.textFile("Book.txt")
words = input_lines.flatMap(normalize_words)
# word_counts = words.countByValue()
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey()
results = word_counts_sorted.collect()

for word in results:
    clean_word = word[1].encode("ascii", "ignore")
    if clean_word:
        print(f"{clean_word.decode()}: \t\t{word[0]}")

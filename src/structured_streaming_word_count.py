# The main functionality of this Spark job was blatantly copied from Spark's own documentation
# ( https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html ) and the
# following improvements were added:
#   * ignore case
#   * filter non-alphanumeric/non-space characters
#   * avoid outputting the word count for an empty word (which occurs when two spaces occur together)
#   * the output by descending word count, sub-sorted by word when there are two or more words with the same word count

import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lower, explode, split, regexp_replace

# Create a Spark session; get its Spark context.
appName = re.sub("\.py$", "", os.path.basename(__file__))
spark = SparkSession.builder.appName(appName).getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")

# Create a structured streaming DataFrame; start it reading lines data from localhost:9999.
linesDF = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the lines into words.
wordsDF = linesDF.select(explode(split(lower(regexp_replace(linesDF.value, "[^a-zA-Z0-9 ]", "")), " ")).alias("word"))

# Generate a running word count.
wordCountsDF = wordsDF.groupBy("word").count().filter("word <> ''").orderBy(col("count").desc(), col("word").asc())

# Start running the query which prints the running counts to the console.
# NOTE: query is of type pyspark.sql.streaming.StreamingQuery.
query = wordCountsDF.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()

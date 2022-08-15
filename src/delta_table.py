import sys
import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta import *

# Create a Spark session; get its Spark context.
appName = re.sub("\.py$", "", os.path.basename(__file__))
spark = SparkSession.builder.appName(appName).getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")

# Calculate some paths relative to the current source file.
script_dir = os.path.abspath(os.path.dirname(__file__))
script_parent_dir = os.path.dirname(script_dir)
test_output_dir = os.path.join(script_parent_dir, "test_output")

# Build a DataFrame with a schema and some data.
data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
schema = StructType([
    StructField('language', StringType(), False),
    StructField("num_users", LongType(), False),
])
df = spark.createDataFrame(data, schema)
print("Original DataFrame:");
df.printSchema()
df.show()

# Create a temporary view from a DataFrame; query it with Spark SQL.
df.createOrReplaceTempView("languages")
sqlDF = spark.sql("select * from languages order by num_users desc")
print("SQL DataFrame:");
sqlDF.printSchema()
sqlDF.show()

# Write a DataFrame to a Delta table inside a folder.
delta_dir = os.path.join(test_output_dir, "delta", "languages_sorted")
sqlDF.write.format("delta").save(delta_dir)

# Read Delta; show its contents.
print("Delta File DataFrame:");
deltaDF = spark.read.format("delta").load(delta_dir)
deltaDF.printSchema()
deltaDF.show()

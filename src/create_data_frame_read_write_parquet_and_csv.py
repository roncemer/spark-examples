import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType

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

# Write a DataFrame to a single-partition Parquet file inside a folder.
parquet_dir = os.path.join(test_output_dir, "parquet", "languages_sorted");
sqlDF.repartition(1).write.parquet(parquet_dir)

# Read Parquet; show its contents.
print("Parquet File DataFrame:");
parquetDF = spark.read.parquet(parquet_dir)
parquetDF.printSchema()
parquetDF.show()

# Write a DataFrame to a CSV file.
csv_dir = os.path.join(test_output_dir, "csv", "languages_sorted");
sqlDF.repartition(1).write.option("header", True).csv(csv_dir)

# Read CSV; show its contents.
print("CSV File DataFrame:");
csvDF = spark.read.option("header", True).csv(csv_dir)
csvDF.printSchema()
csvDF.show()

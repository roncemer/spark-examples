import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Create a Spark session; get its Spark context.
spark = SparkSession.builder.master("local[1]").appName('test').getOrCreate()
sc = SparkContext.getOrCreate()

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
parquet_dir = "test_output/parquet/languages_sorted";
sqlDF.repartition(1).write.parquet(parquet_dir)

# Read Parquet; show its contents.
print("Parquet File DataFrame:");
parquetDF = spark.read.parquet(parquet_dir)
parquetDF.printSchema()
parquetDF.show()

# Write a DataFrame to a CSV file.
csv_dir = "test_output/csv/languages_sorted";
sqlDF.repartition(1).write.option("header", True).csv(csv_dir)

# Read CSV; show its contents.
print("CSV File DataFrame:");
csvDF = spark.read.option("header", True).csv(csv_dir)
csvDF.printSchema()
csvDF.show()

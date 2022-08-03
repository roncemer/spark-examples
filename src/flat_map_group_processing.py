import sys
import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from decimal import Context

# Create a Spark session; get its Spark context.
appName = re.sub("\.py$", "", os.path.basename(__file__))
spark = SparkSession.builder.appName(re.sub("\.py$", "", os.path.basename(__file__))).getOrCreate()
sc = SparkContext.getOrCreate()

# Calculate some paths relative to the current source file.
script_dir = os.path.abspath(os.path.dirname(__file__))
script_parent_dir = os.path.dirname(script_dir)
test_input_dir = os.path.join(script_parent_dir, "test_input")
test_output_dir = os.path.join(script_parent_dir, "test_output")

# Read groups CSV file; adjust its schema; show its contents.
groupsDF = spark.read.option("header", True).csv(os.path.join(test_input_dir, "flat_map_group_processing_groups.csv"))
groupsDF = (
    groupsDF.withColumn("group_no_new", groupsDF["group_no"].cast(IntegerType())).drop("group_no").withColumnRenamed("group_no_new", "group_no")
    .withColumn("total_value_new", groupsDF["total_value"].cast(DecimalType(16, 2))).drop("total_value").withColumnRenamed("total_value_new", "total_value")
)
print("Groups:")
groupsDF.printSchema()
groupsDF.show()

# Read events CSV file; adjust its schema; show its contents.
eventsDF = spark.read.option("header", True).csv(os.path.join(test_input_dir, "flat_map_group_processing_events.csv"))
eventsDF = (
    eventsDF.withColumn("event_time_new", eventsDF["event_time"].cast(TimestampType())).drop("event_time").withColumnRenamed("event_time_new", "event_time")
    .withColumn("group_no_new", eventsDF["group_no"].cast(IntegerType())).drop("group_no").withColumnRenamed("group_no_new", "group_no")
    .withColumn("weight_new", eventsDF["weight"].cast(DecimalType(16, 4))).drop("weight").withColumnRenamed("weight_new", "weight")
)
print("Events:")
eventsDF.printSchema()
eventsDF.show(n=10000)

# Create temporary views.
groupsDF.createOrReplaceTempView('groups');
eventsDF.createOrReplaceTempView('events');

# Execute a Spark SQL join query to get the groups and their events.
# NOTE: This is for informational purposes only.  We'll query each group separately below.
joinedDF = spark.sql("""
select e.*, g.total_value
from events e
inner join groups g on g.group_no = e.group_no
order by e.group_no, e.event_time
""")
print("Joined:")
joinedDF.printSchema()
joinedDF.show(n=10000)

# Build the groups.
groups = []
for g in groupsDF.collect():
    edf = spark.sql(f"select * from events where group_no = {g.group_no} order by event_time")
    events = edf.rdd.map(lambda e: {"event_time": e.event_time, "group_no": e.group_no, "weight": float(e.weight)}).collect()
    groups.append({"group_no": g.group_no, "total_value": float(g.total_value), "events": events})
groupsRDD = spark.sparkContext.parallelize(groups)

def distribute_total_value(group):
    total_value = group["total_value"]

    # Calculate the total weight for all events in the group.
    total_weight = 0.0
    for event in group["events"]:
        total_weight = round(total_weight + event["weight"], 4)

    # Distribute the total value across the events in the group.
    # Calculate total amount distributed.
    # Find the event with the largest absolute distributed amount.
    largest_event = None
    total_distrib = 0.0
    distrib_events = []
    for event in group["events"]:
        value = round((total_value * event["weight"]) / total_weight, 2)
        distrib_event = {
            "event_time": event["event_time"],
            "group_no": event["group_no"],
            "weight": event["weight"],
            "value": value,
        }
        distrib_events.append(distrib_event)
        total_distrib = round(total_distrib + value, 2)
        if largest_event is None or abs(value) > abs(largest_event["value"]):
            largest_event = distrib_event

    # If there is rounding error, put it on the largest event.
    rounding_err = round(total_value - total_distrib, 2)
    if rounding_err != 0.0 and largest_event is not None:
        largest_event["value"] = round(largest_event["value"] + rounding_err, 2)

    return distrib_events

distribRDD = groupsRDD.flatMap(lambda group: distribute_total_value(group))
distribDF = distribRDD.toDF()
distribDF = (
    distribDF.withColumn("weight_new", distribDF["weight"].cast(DecimalType(16, 4))).drop("weight").withColumnRenamed("weight_new", "weight")
    .withColumn("value_new", distribDF["value"].cast(DecimalType(16, 2))).drop("value").withColumnRenamed("value_new", "value")
)
print("Events with distributed values:")
distribDF.printSchema()
distribDF.show(n=10000)
# Create temporary view.
distribDF.createOrReplaceTempView('distrib');

# Show total of total_value across all groups.
df = spark.sql("select sum(total_value) as total_groups_value from groups")
df.show(n=10000)

# Show total of value across all distributed events.
df = spark.sql("select sum(value) as total_distrib_value from distrib")
df.show(n=10000)

# Show the number of input events.
df = spark.sql("select count(*) as num_input_events from events")
df.show(n=10000)

# Show the number of distributed events.
df = spark.sql("select count(*) as num_distrib_events from distrib")
df.show(n=10000)

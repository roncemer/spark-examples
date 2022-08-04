import sys
import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import LongType, DecimalType, TimestampType
from decimal import Context

# Create a Spark session; get its Spark context.
appName = re.sub("\.py$", "", os.path.basename(__file__))
spark = SparkSession.builder.appName(re.sub("\.py$", "", os.path.basename(__file__))).getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")

# Calculate some paths relative to the current source file.
script_dir = os.path.abspath(os.path.dirname(__file__))
script_parent_dir = os.path.dirname(script_dir)
test_input_dir = os.path.join(script_parent_dir, "test_input")

# Read groups CSV file; adjust its schema; show its contents.
groupsDF = spark.read.option("header", True).csv(os.path.join(test_input_dir, "flat_map_group_processing_groups.csv"))
groupsDF = (
    groupsDF.withColumn("group_no_new", groupsDF["group_no"].cast(LongType())).drop("group_no").withColumnRenamed("group_no_new", "group_no")
    .withColumn("total_value_new", groupsDF["total_value"].cast(DecimalType(16, 2))).drop("total_value").withColumnRenamed("total_value_new", "total_value")
)
print("Groups:")
groupsDF.printSchema()
groupsDF.show()

# Read events CSV file; adjust its schema; show its contents.
eventsDF = spark.read.option("header", True).csv(os.path.join(test_input_dir, "flat_map_group_processing_events.csv"))
eventsDF = (
    eventsDF.withColumn("id_new", eventsDF["id"].cast(LongType())).drop("id").withColumnRenamed("id_new", "id")
    .withColumn("event_time_new", eventsDF["event_time"].cast(TimestampType())).drop("event_time").withColumnRenamed("event_time_new", "event_time")
    .withColumn("group_no_new", eventsDF["group_no"].cast(LongType())).drop("group_no").withColumnRenamed("group_no_new", "group_no")
    .withColumn("weight_new", eventsDF["weight"].cast(DecimalType(16, 4))).drop("weight").withColumnRenamed("weight_new", "weight")
)
print("Events:")
eventsDF.printSchema()
eventsDF.show(n=10000)

# Create temporary views.
groupsDF.createOrReplaceTempView('groups');
eventsDF.createOrReplaceTempView('events');

# Show the number of input events.
df = spark.sql("select count(*) as num_input_events from events")
print("number of input events:")
df.printSchema()
df.show()

# Show total of total_value across all groups.
df = spark.sql("select sum(total_value) as total_groups_value from groups")
print("total of total_value across all groups:")
df.printSchema()
df.show()

# ==================================================================================================================
# Method #1 for distributing the total value for each group to the events in the group based on each event's weight.
# This method builds an RDD with one row for each group, containing the group number, the total value for the group,
# and the list of events in that group.  Then, using that RDD, it calls flatMap() with a lambda which calls a
# a function once per group, distributing those function calls across the cluster.  Each function call distributes
# the total value for each group to the events in that group.
# PRO: Pretty fast as long as there aren't a lot of groups.
# PRO: Parallelizable on a per-group basis (multiple groups simultaneously being processed on different nodes or
#      processes within the Spark cluster).
# CON: Requires that all of the events for a given group fit in memory on the driver node / process, because we have
#      to call collect() on the RDD which contains all of the events for the current group.
# CON: Requires a "for" loop in the setup to iterate over each group and query the events for that group.'
#      This setup overhead could possibly negate the parallelism at the group level.
# ==================================================================================================================

# Build the groups.
groupsRDD = None
for g in groupsDF.collect():
    edf = spark.sql(f"select * from events where group_no = {g.group_no} order by event_time, id")
    events = edf.rdd.map(lambda e: {"id": e.id, "event_time": e.event_time, "group_no": e.group_no, "weight": float(e.weight)}).collect()
    newGroupRDD = spark.sparkContext.parallelize([{"group_no": g.group_no, "total_value": float(g.total_value), "events": events}])
    if groupsRDD is None:
        groupsRDD = newGroupRDD
    else:
        groupsRDD = groupsRDD.union(newGroupRDD)

# Define a function for the flatMap() lambda to call to distribute the revenue for each group.
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
            "id": event["id"],
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

# Distribute the revenue in parallel, using flatMap().
fmdistribRDD = groupsRDD.flatMap(lambda group: distribute_total_value(group))
fmdistribDF = fmdistribRDD.toDF()
fmdistribDF = (
    fmdistribDF.withColumn("weight_new", fmdistribDF["weight"].cast(DecimalType(16, 4))).drop("weight").withColumnRenamed("weight_new", "weight")
    .withColumn("value_new", fmdistribDF["value"].cast(DecimalType(16, 2))).drop("value").withColumnRenamed("value_new", "value")
)
# Reorder columns for consistency.
fmdistribDF = fmdistribDF.select("id", "event_time", "group_no", "weight", "value")
print("Events with distributed values:")
fmdistribDF.printSchema()
fmdistribDF.show(n=10000)
fmdistribDF.createOrReplaceTempView('fmdistrib');

# Show total of value across all flatMap() distributed events.
df = spark.sql("select sum(value) as total_flat_map_distrib_value from fmdistrib")
print("total of value across all flatMap() distributed events:")
df.printSchema()
df.show()

# Show the number of flatMap() distributed events.
df = spark.sql("select count(*) as num_distrib_events from fmdistrib")
print("number of flatMap() distributed events:")
df.printSchema()
df.show()

# ==================================================================================================================
# Method #2 for distributing the total value for each group to the events in the group based on each event's weight.
# This method uses Spark SQL and a three-step process:
#     1) Do a rough distribution, ignoring rounding error, using a join query.
#     2) For each group, calculate the rounding error and find the event with the largest absolute value.
#     3) Add the rounding errors to the correct events by using a left join between the rough-distributed events
#        and the rounding error table.
# PRO: Uses Spark SQL to do everything.
# PRO: May scale better across the cluster regardless of the number of groups or actions.
# PRO: Doesn't require a lot of memory on the driver node / process.
# PRO: Doesn't and doesn't require any iteration over groups in the setup (there really is no setup).
# ==================================================================================================================

# First step: do a rough distribution, ignoring rounding error.
sqldistribDF = spark.sql("""
select e.*, round((g.total_value * e.weight) / gtw.total_weight, 2) as value
from events e
inner join (select group_no, round(sum(weight), 4) as total_weight from events group by group_no) gtw on gtw.group_no = e.group_no
inner join groups g on g.group_no = e.group_no
order by e.group_no, e.event_time, e.id
""")
print("First-pass SQL distribution:")
sqldistribDF.printSchema()
sqldistribDF.show(n=10000)
sqldistribDF.createOrReplaceTempView('sqldistrib');

# Show total of value across all first-pass SQL distributed events.
df = spark.sql("select sum(value) as total_sql_first_step_distrib_value from sqldistrib")
print("total of value across all first-pass SQL distributed events:")
df.printSchema()
df.show()

# Second step: calculate the rounding error and largest event per group.
roundingerrDF = spark.sql("""
select g.*, g.total_value - gt.dist_total_value as rounding_error, gm.id
from groups g
inner join (select group_no, sum(value) as dist_total_value from sqldistrib group by group_no) gt on gt.group_no = g.group_no
inner join (
  select * from (select id, group_no, row_number() over (partition by group_no order by abs(value) desc) as rn from sqldistrib) where rn = 1
) gm on gm.group_no = g.group_no
where g.total_value <> gt.dist_total_value
order by g.group_no
""")
print("Rounding error and largest event id for each group:")
roundingerrDF.printSchema()
roundingerrDF.show()
roundingerrDF.createOrReplaceTempView('roundingerr');

# Show the total rounding error.
df = spark.sql("select sum(rounding_error) as total_rounding_error from roundingerr")
print("total rounding error:")
df.printSchema()
df.show()

# Third step: add the rounding error for each group to the largest event in the group.
sqldistribDF = spark.sql("""
select e.id, e.event_time, e.group_no, e.weight, e.value + ifnull(re.rounding_error, 0.0) as value
from sqldistrib e
left join roundingerr re on re.group_no = e.group_no and re.id = e.id
order by e.group_no, e.event_time, e.id
""")
print("SQL distribution after fixing rounding error:")
sqldistribDF.printSchema()
sqldistribDF.show(n=10000)
sqldistribDF.createOrReplaceTempView('sqldistrib');

# Show total of value across all SQL distributed events.
df = spark.sql("select sum(value) as total_sql_distrib_value from sqldistrib")
print("total of value across all SQL distributed events:")
df.printSchema()
df.show()

# Show the number of SQL distributed events.
df = spark.sql("select count(*) as num_distrib_events from sqldistrib")
print("number of SQL distributed events:")
df.printSchema()
df.show()

# ==================================================================================================================
# Compare the results of the flatMap() distribution algorithm against the results of the SQL distribution algorithm.
# The two should be identical.
# ==================================================================================================================

# Show the rows which are in the flatMap() distributed DataFrame but not in the SQL distributed DataFrame.
print("rows which are in the flatMap() distributed DataFrame but not in the SQL distributed DataFrame:")
fmdistribDF.subtract(sqldistribDF).show(n=10000)

# Show the rows which are in the SQL distributed DataFrame but not in the flatMap() distributed DataFrame.
print("rows which are in the SQL distributed DataFrame but not in the flatMap() distributed DataFrame:")
sqldistribDF.subtract(fmdistribDF).show(n=10000)

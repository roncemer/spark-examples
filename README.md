# Spark Examples
Examples of how to use Apache Spark to do various data processing tasks, with the workload spread across a Spark cluster.

## Install prerequisites (Mac)

1. Install Homebrew.  See here: https://brew.sh/
2. Install Apache Spark and Python3.
   ```console
   brew install apache-spark python3
   ```
3. Install PySpark.
   ```console
   pip3 install pyspark
   ```

## Basic DataFrame handling, reading and writing Parquet and CSV files
This Spark job creates a DataFrame with a schema and some data, create a temporary view, queries the temporary view, saves the results to a single-partition Parquet file, reads the Parquet file, saves to a single-partition CSV file, and reads the CSV file.
```console
./cleanup ; spark-submit src/create_data_frame_read_write_parquet_and_csv.py
```

## Execute calculations in parallel, using flatMap()
This Spark job uses flatMap() to execute calculation jobs in parallel across a Spark cluster, passing a lookup table to the workers as a broadcast variable.
```console
spark-submit src/flat_map_with_broadcast_var.py
```

## Distribute the total values of groups to events in the groups based on event's weights
This Spark job takes as input a number of groups and their corresponding total values, and a list of events, with each event belonging to one group.  It demonstrates two methods to implement such an algorithm.

Method #1 builds an RDD with one row for each group, containing the group number, the total value for the group, and the list of events in that group.  Then, using that RDD, it calls flatMap() with a lambda which calls a function once per group, distributing those function calls across the cluster.  Each function call distributes the total value for each group to the events in that group.

Method #2 uses Spark SQL and a three-step process:
  1. Do a rough distribution, ignoring rounding error, using a join query.
  2. For each group, calculate the rounding error and find the event with the largest absolute value.
  3. Add the rounding errors to the correct events by using a left join between the rough-distributed events and the rounding error table.

```console
spark-submit src/group_value_distribution_by_weights.py
```

## Clean up output files which were created by demos
This script cleans up the files which are created by demos which save files.  It should be run before any demos which creates output files.
```console
./cleanup
```

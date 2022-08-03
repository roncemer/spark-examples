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

## Process groups of events in parallel, using flatMap()
This Spark job takes as input a number of groups and their corresponding total values, and a list of events, with each event belonging to one group.  It then uses flatMap() to execute calculation jobs in parallel across a Spark cluster, where each job corresponds to a single group.  Each job distributes the group's total value across the events in that group based on each event's weight.
```console
spark-submit src/flat_map_group_processing.py
```

## Clean up output files which were created by demos
This script cleans up the files which are created by demos which save files.  It should be run before any demos which creates output files.
```console
./cleanup
```

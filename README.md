# Spark Examples
Examples of how to use Apache Spark to do various data processing tasks, with the workload spread across a Spark cluster.

## Install prerequisiets (Mac)

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

## Executing calculations in parallel
This Spark job uses flatMap() to execute calculation jobs in parallel across a Spark cluster, passing a lookup table to the workers as a broadcast variable.
```console
spark-submit src/flat_map_with_broadcast_var.py
```

## Clean up output files which were created by demos
This script cleans up the files which are created by demos which save files.  It should be run before any demos which creates output files.
```console
./cleanup
```

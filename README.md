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


## Group value distribution among events in the group, based on event weights
The problem: Given multiple groups and multiple events within each group, distribute the total value for each group to events in the group based on the events' relative weights.

This Spark job takes as input a number of groups and their corresponding total values, and a list of events, with each event belonging to one group.  Then, for each group, it uses the relative weights for each of the events in the group to determine an individual event's share of the total value of the group.  In other words, for each group, the group's total value is distributed across the events in that group based on the events' weights relative to one another.

This Spark job demonstrates two methods to implement such an algorithm.

Method #1 uses a Spark SQL join query and map() / reduce() / parallelize() to create an RDD with one row for each group, containing the group number, the total value for the group, and the list of events in that group.  Then, using that RDD, it calls flatMap() with a lambda which calls a function once per group, distributing those function calls across the cluster.  Each function call distributes the total value for each group to the events in that group.

Method #2 uses Spark SQL and a three-step process:
  1. Do a rough distribution, ignoring rounding error, using a join query.
  2. For each group, calculate the rounding error and find the event with the largest absolute value.
  3. Add the rounding errors to the correct events by using a left join between the rough-distributed events and the rounding error table.

After both methods have executed, the two resulting DataFrames are subtracted from each other, and the non-matching rows are shown.  Since the results are both empty, it is proven that both methods produce exactly the same result.

To run the job:
```console
spark-submit src/group_value_distribution_by_weights.py
```

The Method #1 implementation also has an option to only fit one group and its events into memory on the driver node / process, reducing the probability that the job will fail with an out-of-memory error.  With this option turned on, the setup takes a bit longer to run because each group is built up separately and then converted to an RDD which is distributed across the cluster.  To run the job with this option enabled:
```console
spark-submit src/group_value_distribution_by_weights.py --conserve-driver-memory true
```


## Read lines from a network socket, output a running word count by distinct words (Spark Structured Streaming)

This Spark job reads lines from a network socket, breaks the lines into words, and outputs a running word count by word to the console.  When new text comes in, the counts are updated.

The main functionality of this Spark job was blatantly copied from Spark's own documentation
( https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html ) and the
following improvements were added:
  - ignore case
  - filter non-alphanumeric/non-space characters
  - avoid outputting the word count for an empty word (which occurs when two spaces occur together)
  - the output by descending word count, sub-sorted by word when there are two or more words with the same word count

In a separate terminal window, start up a socket server using Netcat:
```console
nc -lk 9999
```

Start up the Spark job:
```console
spark-submit src/structured_streaming_word_count.py
```

Wait until you get the first (empty) word count report in the Spark console:
> -------------------------------------------
> Batch: 0
> -------------------------------------------
> +----+-----+
> |word|count|
> +----+-----+
> +----+-----+

Copy and paste the following text into the Netcat window, then press Enter:
> hello.  this is a test; i am testing THIS thing.

After a few seconds, you should see the first non-empty word count report in the Spark console:
> -------------------------------------------
> Batch: 1
> -------------------------------------------
> +-------+-----+
> |   word|count|
> +-------+-----+
> |   this|    2|
> |      a|    1|
> |     am|    1|
> |  hello|    1|
> |      i|    1|
> |     is|    1|
> |   test|    1|
> |testing|    1|
> |  thing|    1|
> +-------+-----+

Next copy and paste the following text into the Netcat window, then press Enter:
> hello.  this is a test; i am testing THIS thing.  i hope it works.

After a few seconds, you should see this:
> -------------------------------------------
> Batch: 2
> -------------------------------------------
> +-------+-----+
> |   word|count|
> +-------+-----+
> |   this|    4|
> |      i|    3|
> |      a|    2|
> |     am|    2|
> |  hello|    2|
> |     is|    2|
> |   test|    2|
> |testing|    2|
> |  thing|    2|
> |   hope|    1|
> |     it|    1|
> |  works|    1|
> +-------+-----+

You can stop the Spark job by pressing *Control+C* in its window, and stop Netcat by pressing *Control+C* in its window.


## Clean up output files which were created by demos
This script cleans up the files which are created by demos which save files.  It should be run before any demos which creates output files.
```console
./cleanup
```

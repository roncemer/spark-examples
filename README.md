# Spark Examples

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

## Create a DataFrame with a schema and some data; create a temporary view; query the temporary view; save to single-partition Parquet file; read Parquet; save to CSV; read CSV
```console
./cleanup ; spark-submit src/create_data_frame_read_write_parquet_and_csv.py
```




## Clean up output files which were created by demos.
```console
./cleanup
```

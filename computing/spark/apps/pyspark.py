from pyspark.sql import SparkSession

# Create Spark session that connects to the Spark master
spark = SparkSession.builder \
    .appName("PySpark-Docker-Cluster") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Example operation: Load a CSV file from the shared volume into a DataFrame
# Assuming the data is in /opt/spark-data (as mapped to ./data in your local system)
df = spark.read.csv("/opt/spark-data/your_data.csv", header=True, inferSchema=True)

# Perform basic DataFrame operations (example)
df.show()  # Show some rows of the DataFrame

# Do some transformation or actions
df_filtered = df.filter(df['column_name'] > 10)

# Show the filtered result
df_filtered.show()

# Stop the Spark session after processing
spark.stop()

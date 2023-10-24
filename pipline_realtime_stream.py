import findspark
findspark.init()

from pyspark.sql import SparkSession

# Создайте объект SparkSession
spark = SparkSession.builder \
    .appName("PostgresToJSONStreaming") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://your_db_host:your_db_port/your_db_name"
connection_properties = {
    "user": "your_db_user",
    "password": "your_db_password",
    "driver": "org.postgresql.Driver"
}

df = spark.read \
    .jdbc(url=jdbc_url, table="your_table", properties=connection_properties)

query = df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output") \
    .start()

query.awaitTermination()

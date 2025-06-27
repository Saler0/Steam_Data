from pyspark.sql import SparkSession
import os

print("JAVA_HOME =", os.environ.get("JAVA_HOME"))
spark = SparkSession.builder.appName("TestJava").getOrCreate()
print("Spark initialized")
spark.stop()
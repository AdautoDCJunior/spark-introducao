from pyspark.sql import SparkSession
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

print(spark)

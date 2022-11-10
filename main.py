import os
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('StartingSpark') \
    .config('spark.driver.host', 'localhost') \
    .config('spark.ui.port', '4050') \
    .getOrCreate()


basePath = os.getcwd()
pathCompany = os.path.join(basePath, 'data', 'empresas', '*.csv')
pathEstablishment = os.path.join(basePath, 'data', 'estabelicimento', '*.csv')
pathPartners = os.path.join(basePath, 'data', 'socios', '*.csv')

dfCompany = spark.read.csv(pathCompany, sep=';', inferSchema=True)
dfEstablishment = spark.read.csv(pathEstablishment, sep=';', inferSchema=True)
dfPartners = os.path.join(pathPartners, 'data', '*.csv')

print(dfCompany.count())
print(dfEstablishment.count())
print(dfPartners.count())
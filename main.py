import os
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import DoubleType, StringType
import json

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('StartingSpark') \
    .config('spark.driver.host', 'localhost') \
    .config('spark.ui.port', '4050') \
    .getOrCreate()


basePath = os.getcwd()
pathCompany = os.path.join(basePath, 'data', 'empresas', '*.csv')
pathEstablishment = os.path.join(basePath, 'data', 'estabelecimentos', '*.csv')
pathPartners = os.path.join(basePath, 'data', 'socios', '*.csv')

dfCompany = spark.read.csv(pathCompany, sep=';', inferSchema=True)
dfEstablishment = spark.read.csv(pathEstablishment, sep=';', inferSchema=True)
dfPartners = spark.read.csv(pathPartners, sep=';', inferSchema=True)

colNames = json.load(open('./colNames.json'))

colNamesCompany = colNames['company']
for index, colName in enumerate(colNamesCompany):
    dfCompany = dfCompany.withColumnRenamed(f'_c{index}', colName.upper())

colNamesEstablishment = colNames['Establishment']
for index, colName in enumerate(colNamesEstablishment):
    dfEstablishment = dfEstablishment.withColumnRenamed(f'_c{index}', colName.upper())

colNamesPartners = colNames['Partners']
for index, colName in enumerate(colNamesPartners):
    dfPartners = dfPartners.withColumnRenamed(f'_c{index}', colName.upper())

dfCompany = dfCompany.withColumn('CAPITAL_SOCIAL', f.regexp_replace('CAPITAL_SOCIAL', ',', '.'))
dfCompany = dfCompany.withColumn('CAPITAL_SOCIAL', dfCompany['CAPITAL_SOCIAL'].cast(DoubleType()))

dfCompany.printSchema()
print(dfCompany.limit(5).toPandas())

dfEstablishment = dfEstablishment \
    .withColumn(
        'DATA_SITUACAO_CADASTRAL', 
        f.to_date(dfEstablishment['DATA_SITUACAO_CADASTRAL'].cast(StringType()), 'yyyyMMdd')
    ).withColumn(
        'DATA_DE_INICIO_ATIVIDADE',
        f.to_date(dfEstablishment['DATA_DE_INICIO_ATIVIDADE'].cast(StringType()), 'yyyyMMdd')
    ).withColumn(
        'DATA_DA_SITUACAO_ESPECIAL',
        f.to_date(dfEstablishment['DATA_DA_SITUACAO_ESPECIAL'].cast(StringType()), 'yyyyMMdd')
    )

dfEstablishment.printSchema()
print(dfEstablishment.limit(5).toPandas())

dfPartners = dfPartners.withColumn(
    'DATA_DE_ENTRADA_SOCIEDADE',
    f.to_date(dfPartners['DATA_DE_ENTRADA_SOCIEDADE'].cast(StringType()), 'yyyyMMdd')
)

dfPartners.printSchema()
print(dfPartners.limit(5).toPandas())
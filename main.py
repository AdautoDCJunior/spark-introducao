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

dfPartners = dfPartners.withColumn(
    'DATA_DE_ENTRADA_SOCIEDADE',
    f.to_date(dfPartners['DATA_DE_ENTRADA_SOCIEDADE'].cast(StringType()), 'yyyyMMdd')
)

# dfCompany.printSchema()
# print(dfCompany.limit(5).toPandas())

# dfEstablishment.printSchema()
# print(dfEstablishment.limit(5).toPandas())

# dfPartners.printSchema()
# print(dfPartners.limit(5).toPandas())

# print(dfCompany.select('NATUREZA_JURIDICA', 'PORTE').toPandas())

# print(dfPartners.select(
#     'NOME_DO_SOCIO_OU_RAZAO_SOCIAL', 
#     'FAIXA_ETARIA',
#     f.year('DATA_DE_ENTRADA_SOCIEDADE').alias('ANO_DE_ENTRADA')
# ).toPandas())

# print(dfEstablishment.select(
#     'NOME_FANTASIA',
#     'MUNICIPIO',
#     f.year('DATA_DE_INICIO_ATIVIDADE').alias('ANO_INICIO_ATIVIDADE'),
#     f.month('DATA_DE_INICIO_ATIVIDADE').alias('MES_INICIO_ATIVIDADE')
# ).toPandas())

# dfPartners.printSchema()

# dfPartners.select(
#     [f.count(f.when(f.isnull(c), 1)).alias(c) for c in dfPartners.columns]
# ).show()

# dfPartners = dfPartners.na.fill(0).limit(5).show()
# dfPartners = dfPartners.na.fill('-').limit(5).show()

# dfPartners \
#     .select(
#         'NOME_DO_SOCIO_OU_RAZAO_SOCIAL', 
#         'FAIXA_ETARIA',
#         f.year('DATA_DE_ENTRADA_SOCIEDADE').alias('ANO_DE_ENTRADA')
#     ).orderBy(['ANO_DE_ENTRADA', 'FAIXA_ETARIA'], ascending=[False, True]) \
#     .show(10, False)

# dfCompany \
#     .where('CAPITAL_SOCIAL == 50') \
#     .show(10, False)

# dfPartners\
#     .select('NOME_DO_SOCIO_OU_RAZAO_SOCIAL')\
#     .filter(dfPartners['NOME_DO_SOCIO_OU_RAZAO_SOCIAL'].startswith('RODRIGO'))\
#     .filter(dfPartners['NOME_DO_SOCIO_OU_RAZAO_SOCIAL'].endswith('DIAS'))\
#     .show(10, False)

# df = spark.createDataFrame([('RESTAURANTE DO RUI',), ('Juca restaurantes ltda',), ('Joca Restaurante',)], ['data'])

# df.where(f.upper(df['data']).like('RESTAURANTE%')).show(truncate=False)

dfCompany\
    .select( 
        'RAZAO_SOCIAL', 
        'NATUREZA_JURIDICA', 
        'PORTE',
        'CAPITAL_SOCIAL',
    )\
    .filter(f.upper(dfCompany['RAZAO_SOCIAL']).like('%RESTAURANTE%'))\
    .show(10, False)
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

# dfCompany\
#     .select( 
#         'RAZAO_SOCIAL', 
#         'NATUREZA_JURIDICA', 
#         'PORTE',
#         'CAPITAL_SOCIAL',
#     )\
#     .filter(f.upper(dfCompany['RAZAO_SOCIAL']).like('%RESTAURANTE%'))\
#     .show(10, False)

# dfPartners\
#     .select(f.year('DATA_DE_ENTRADA_SOCIEDADE').alias('ANO_DE_ENTRADA'))\
#     .filter('ANO_DE_ENTRADA >= 2010')\
#     .groupBy('ANO_DE_ENTRADA')\
#     .count()\
#     .orderBy('ANO_DE_ENTRADA', ascending=True)\
#     .show(truncate=False)

# dfCompany\
#     .select('CNPJ_BASICO', 'PORTE', 'CAPITAL_SOCIAL')\
#     .groupBy('PORTE')\
#     .agg(
#         f.avg('CAPITAL_SOCIAL').alias('CAPITAL_SOCIAL_MEDIO'),
#         f.count('CNPJ_BASICO').alias("FREQUENCIA")
#     )\
#     .orderBy('PORTE', ascending=True)\
#     .show(truncate=False)

# dfCompany\
#     .select('CAPITAL_SOCIAL')\
#     .summary()\
#     .show(truncate=False)


# produtos = spark.createDataFrame(
#     [
#         ('1', 'Bebidas', 'Água mineral'), 
#         ('2', 'Limpeza', 'Sabão em pó'), 
#         ('3', 'Frios', 'Queijo'), 
#         ('4', 'Bebidas', 'Refrigerante'),
#         ('5', 'Pet', 'Ração para cães')
#     ],
#     ['id', 'cat', 'prod']
# )

# impostos = spark.createDataFrame(
#     [
#         ('Bebidas', 0.15), 
#         ('Limpeza', 0.05),
#         ('Frios', 0.065),
#         ('Carnes', 0.08)
#     ],
#     ['cat', 'tax']
# )

# produtos\
#     .join(impostos, 'cat', how='inner')\
#     .sort('id')\
#     .show(truncate=True)

# produtos\
#     .join(impostos, 'cat', how='left')\
#     .sort('id')\
#     .show(truncate=True)

# produtos\
#     .join(impostos, 'cat', how='right')\
#     .sort('id')\
#     .show(truncate=True)

# produtos\
#     .join(impostos, 'cat', how='outer')\
#     .sort('id')\
#     .show(truncate=True)

joinCompanies = dfEstablishment.join(dfCompany, 'CNPJ_BASICO', how='inner')

# # joinCompanies.printSchema()

# freq = joinCompanies\
#     .select(
#         'CNPJ_BASICO',
#         f.year('DATA_DE_INICIO_ATIVIDADE').alias('DATA_DE_INICIO')
#     )\
#     .where('DATA_DE_INICIO >= 2010')\
#     .groupBy('DATA_DE_INICIO')\
#     .agg(
#         f.count('CNPJ_BASICO').alias('FREQUENCIA')
#     )\
#     .orderBy('DATA_DE_INICIO', ascending=True)

# # freq.show(truncate=True)

# freq.union(
#     freq.select(
#         f.lit('Total').alias('DATA_DE_INICIO'),
#         f.sum(freq['FREQUENCIA']).alias('FREQUENCIA')
#     )
# ).show(truncate=True)

# dfCompany.createOrReplaceTempView('companyView')
# joinCompanies.createOrReplaceTempView('joinCompaniesView')

# spark.sql('SELECT * FROM companyView').show(5, False)

# spark.sql('''
#     SELECT *
#         FROM companyView
#         WHERE CAPITAL_SOCIAL = 50
# ''').show(5)

# spark.sql('''
#         SELECT PORTE, MEAN(CAPITAL_SOCIAL) AS MEDIA 
#             FROM companyView 
#             GROUP BY PORTE
#     ''')\
#     .show(5)

# freq = spark.sql('''
#     SELECT YEAR(DATA_DE_INICIO_ATIVIDADE) AS DATA_DE_INICIO, COUNT(CNPJ_BASICO) AS COUNT
#         FROM joinCompaniesView 
#         WHERE YEAR(DATA_DE_INICIO_ATIVIDADE) >= 2010
#         GROUP BY DATA_DE_INICIO
#         ORDER BY DATA_DE_INICIO
# ''')

# freq.createOrReplaceTempView('freqView')

# spark.sql('''
#     SELECT *
#         FROM freqView
#     UNION ALL
#     SELECT 'Total' AS DATA_DE_INICIO, SUM(COUNT) AS COUNT
#         FROM freqView
# ''').show()

# freq.show()

# writePathCompanyCSV = os.path.join(basePath, 'data', 'empresas', 'csv')
# dfCompany.write.csv(
#     path=writePathCompanyCSV,
#     mode='overwrite',
#     sep=';',
#     header=True
# )

# writePathEstablishmentCSV = os.path.join(basePath, 'data', 'estabelecimentos', 'csv')
# dfEstablishment.write.csv(
#     path=writePathEstablishmentCSV,
#     mode='overwrite',
#     sep=';',
#     header=True
# )

# writePathPartnersCSV = os.path.join(basePath, 'data', 'socios', 'csv')
# dfPartners.write.csv(
#     path=writePathPartnersCSV,
#     mode='overwrite',
#     sep=';',
#     header=True
# )

# writePathCompanyORC = os.path.join(basePath, 'data', 'empresas', 'orc')
# dfCompany.write.parquet(
#     path=writePathCompanyORC,
#     mode='overwrite'
# )

# writePathEstablishmentORC = os.path.join(basePath, 'data', 'estabelecimentos', 'orc')
# dfEstablishment.write.orc(
#     path=writePathEstablishmentORC,
#     mode='overwrite',
# )

# writePathPartnersORC = os.path.join(basePath, 'data', 'socios', 'orc')
# dfPartners.write.parquet(
#     path=writePathPartnersORC,
#     mode='overwrite',
# )

# writePathCompanyCsvAll = os.path.join(basePath, 'data', 'empresas', 'csv-unico')
# dfCompany.coalesce(1).write.csv(
#     path=writePathCompanyCsvAll,
#     mode='overwrite',
#     sep=';',
#     header=True
# )

writePathCompanyOrcPartitionBy = os.path.join(basePath, 'data', 'empresas', 'orc-partitionBy')
dfCompany.write.orc(
    path=writePathCompanyOrcPartitionBy,
    mode='overwrite',
    partitionBy='PORTE'
)

# dfCompanyParquet = spark.read.parquet(writePathCompanyParquet)

# dfCompanyParquet.printSchema()

spark.stop()
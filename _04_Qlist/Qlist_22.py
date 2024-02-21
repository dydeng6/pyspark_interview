from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, coalesce

spark = SparkSession.builder.appName('Qlist_22').getOrCreate()

data = [('Tom', ['Apple', 'Banana']), ('Jerry', ['Pair', 'Orange', 'Chicken']), ('Kate', ['Pork', 'lamb'])]
schema = ['name', 'hobbies']
df = spark.createDataFrame(data, schema=schema)

df1 = df.select(col('name'), explode(col('hobbies')))

df1.show()

print('=' * 50)

data1 = [('a', '', 'b'), ('', 'c', None), (None, '', 'd')]
schema1 = ['col1', 'col2', 'col3']

df_c = spark.createDataFrame(data1, schema1)
df_None = (df_c.withColumn('col1', when(col('col1') == '', None).otherwise(col('col1')))
           .withColumn('col2', when(col('col2') == '', None).otherwise(col('col2')))
           .withColumn('col3', when(col('col3') == '', None).otherwise(col('col3')))
           )
df_None.withColumn('result',coalesce('col1', 'col2', 'col3')).select('result').show()


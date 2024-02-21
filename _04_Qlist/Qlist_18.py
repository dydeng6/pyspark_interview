from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test_17').getOrCreate()
from pyspark.sql.functions import col,min,max
data = [
 (1, ),
 (2,),
 (3,),
 (6,),
 (7,),
 (8,)]
schema="Id int"
df = spark.createDataFrame(data,schema=schema)
df_max_min = df.select(min('Id').alias('min_id'),max('Id').alias('max_id'))

# print(df_max_min.first()[0])
df_full = spark.range(df_max_min.first()['min_id'],df_max_min.first()[1]+1)

df_full.subtract(df).show()
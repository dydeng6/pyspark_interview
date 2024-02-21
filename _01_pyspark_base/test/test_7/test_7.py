from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode

spark = SparkSession.builder.appName('test_7').getOrCreate()

df = spark.read.text('./test_7.txt')
df2 = df.withColumn('new_value',regexp_replace('value','(.*?\\-){3}',"$0,")).drop('value')
df3 = df2.select(explode(split('new_value','-,')))
df3.show()
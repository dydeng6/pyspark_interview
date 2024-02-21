from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import count, col, when
from setuptools.command.alias import alias

spark = SparkSession.builder.appName('test_5').getOrCreate()
df = spark.read.option("nullValue","null").option("header",True).csv('./test_5.csv')
df_count = df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
df_count.show()
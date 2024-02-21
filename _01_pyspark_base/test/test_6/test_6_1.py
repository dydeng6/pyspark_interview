from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark = SparkSession.builder.appName('test_6_1').getOrCreate()
df = spark.read.option("header",True).csv('test_6.csv')
df_split = df.withColumn("Chinese",split(df.Marks,'\\|')[0]).withColumn("English",split(df.Marks,'\\|')[1]).withColumn("Russia",split(df.Marks,'\\|')[1])
df_split.drop(df_split.Marks).show()

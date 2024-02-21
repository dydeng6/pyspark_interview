from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

spark = SparkSession.builder.appName('test_6_1').getOrCreate()
df = spark.read.option("header",True).csv('test_6.csv')
df2 = df.select('ID','Name','Age',explode(split(df.Marks,'\\|')))
df2.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .appName('test_3') \
    .getOrCreate()

simpleData = [("Tom","U1234566"),("Bob","1234566H"),("Jack","1234566")]
schema = ["Name","Mobile"]

df = spark.createDataFrame(data=simpleData, schema=schema)

df2 = df.select("*").filter(col("Mobile").rlike('^[0-9]*$'))

df2.show()


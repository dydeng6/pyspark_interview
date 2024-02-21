from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder \
    .appName('test_2') \
    .getOrCreate()


simpleData = [(1,["Tom","Jerry"]),(2,["Bob","Jack"]),(3,["Carl","Andy"]),(4,["Jim"])]
schema = ["ID","Name"]

df_1 = spark.createDataFrame(data=simpleData,schema=schema)

df_output = df_1.select(col("ID"),explode(col("Name")))

df_output.show()



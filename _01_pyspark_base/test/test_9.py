from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col

spark = SparkSession.builder.appName('test_9').getOrCreate()
data = [(1,'Tom',31),(None,'Jim',None),(2,None,22),(3,'Tim',None),(3,None,None)]
schema = ['ID', 'Name','Age']

df = spark.createDataFrame(data=data,schema=schema)
# check number of null values
df_null = df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
df_null.show()
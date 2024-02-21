from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('test_13').getOrCreate()
data = [(1, "abc@gmail.com"), (2, "bcd@gmail.com"), (3, "abc@gmail.com")]
schema = "ID int,email string"
df = spark.createDataFrame(data, schema)

df.groupBy("email").count().filter(col('count')>1).select('email').show()

# df.show()
print('*' * 60)
df.createOrReplaceTempView('test_13')
df1 = spark.sql('select email from test_13 group by email having count(1) > 1')
df1.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName('test_10').getOrCreate()
data = [('ABSHFJFJ12QWERT12', 1), ('QWERT5674OTUT1', 2), ('DGDGNJDJ1234UYI', 3)]
df = spark.createDataFrame(data, schema="input_string string,id int")
df.show()
df2 = df.select('*').withColumn('new_col',regexp_extract('input_string','(^[a-zA-Z]*[0-9]*)',0)).withColumn('new_col1',regexp_extract('input_string','(^[a-zA-Z]*[0-9]*)([a-zA-Z]*[0-9]*$)',2))
df2.show()

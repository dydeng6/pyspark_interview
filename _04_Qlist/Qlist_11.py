from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

spark = SparkSession.builder.appName('test_11').getOrCreate()
data = [(1, 'War', 'great 3D', 8.9)
    , (2, 'Science', 'fiction', 8.5)
    , (3, 'irish', 'boring', 6.2)
    , (4, 'Ice song', 'Fantacy', 8.6)
    , (5, 'House card', 'Interesting', 9.1)]
schema = "ID int,movie string,description string,rating double"
df = spark.createDataFrame(data, schema)
df.show()
df.select('*').filter((col('ID')%2!=0) & (trim(lower(col('description')))!='boring')).show()

print('*' * 60)
df.createOrReplaceTempView('movies')
df2 = spark.sql('select * from movies where id %2 !=0')
df2.show()
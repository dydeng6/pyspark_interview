from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test_8').getOrCreate()
data = [("北京", "2021-01-01", 100),
        ("北京", "2021-01-02", 200),
        ("上海", "2021-01-01", 150),
        ("上海", "2021-01-02", 300)]
schema = ['地点','日期','销售额']

df = spark.createDataFrame(data=data, schema=schema)
df.show()
df1 = df.groupBy('地点').pivot('日期').sum('销售额')
df1.show()
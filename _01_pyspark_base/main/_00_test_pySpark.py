from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('TestApp') \
    .getOrCreate()

# 创建一个简单的DataFrame
data = [('John', 'Doe', 30), ('Jane', 'Doe', 25)]
df = spark.createDataFrame(data, ["FirstName", "LastName", "Age"])

# 打印DataFrame
df.show()
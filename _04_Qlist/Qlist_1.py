from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .appName('TestApp') \
    .getOrCreate()

def print_fibonacci(n):
    a, b = 0, 1
    while a <= n:
        print(a, end=' ')
        a, b = b, a+b

# print_fibonacci(12)

simpleData_2 = [(5,"Raj","CSE","HP"),(7,"Kual","Mech","Raja")]
columns_2 = ["ID","StuendentName","Department","City"]
df_2 = spark.createDataFrame(data=simpleData_2,schema=columns_2)

df_2.show()

simpleData_1 = [(1,"Tom","BU","XIAN",90),(4,"Jerry","XU","CD",11)]
columns_1 = ["ID","StuendentName","Department","City","Marks"]
df_1 = spark.createDataFrame(data=simpleData_1,schema=columns_1)
df_1.show()
print('='*40)
# print(df.collect())
# df_1.show()
# print("*" * 30)
# df_2.show()

# df_1_d = spark.createDataFrame(df_1.drop("Marks").collect())
# print(df_1_d)
# print(type(df_1_d))
# df = df_1_d.union(df_2).show()
# df.show()
# add a new column to the df_2
df_2 = df_2.withColumn("Marks",lit("null"))
df = df_1.union(df_2)
df.show()
spark.stop()
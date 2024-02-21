# ''' pyspark code
# input = 'aabbbcddeehhhhh'
# # output = [2,3,1,2,2,4]
# '''
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder.appName('test_19_2').getOrCreate()

# 定义函数
def f_counters(input):
    counter = 1
    output = []
    for i in range(0, len(input)-1):
        if(input[i] == input[i+1]):
            counter += 1
        else:
            output.append((input[i],counter))
            counter = 1
    output.append((input[-1],counter))
    return output

# 创建 UDF
schema = ArrayType(StructType([
    StructField("char", StringType(), nullable=True),
    StructField("count", IntegerType(), nullable=True)
]))
# 使用udf函数将 Python 函数转换为 UDF
# 用户自定义函数，这个函数接受一个输入，通过 f_counters 函数计算后返回结果，且返回结果的类型为schema
# f_counters具体的参数，在调用的时候传入
f_counters_udf = udf(f_counters, schema)

# 创建 dataframe 测试
data = [("aabbbcddeehhhhh",),("aaabbbccc",)]
df = spark.createDataFrame(data, ["input"])
df.show()
# 应用 UDF
df.withColumn("counts", f_counters_udf(df["input"])).show(truncate=False)
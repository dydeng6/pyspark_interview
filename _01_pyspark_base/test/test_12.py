from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, collect_list, struct

spark = SparkSession.builder.appName('test_12').getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  # 启用 eager evaluation
spark.conf.set("spark.sql.repl.eagerEval.truncate", 100)  #设置 DataFrame 的列宽
data = [
    ("john", "tomato", 2),
    ("𝚋𝚒𝚕𝚕", "𝚊𝚙𝚙𝚕𝚎", 2),
    ("john", "𝚋𝚊𝚗𝚊𝚗𝚊", 2),
    ("john", "tomato", 3),
    ("𝚋𝚒𝚕𝚕", "𝚝𝚊𝚌𝚘", 2),
    ("𝚋𝚒𝚕𝚕", "𝚊𝚙𝚙𝚕𝚎", 2)
]
schema = "name string,item string,weight int"
df = spark.createDataFrame(data, schema)

df_gb = df.groupBy('name','item').agg(sum(df.weight).alias('sum_weight'))
df_final = df_gb.groupBy('name').agg(collect_list(struct('item','sum_weight')).alias('items'))

df_final.show()
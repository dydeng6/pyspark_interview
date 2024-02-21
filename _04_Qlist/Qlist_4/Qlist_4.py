from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext

# spark = SparkSession.builder \
#     .appName('Qlist_4') \
#     .getOrCreate()
# df = spark.read.csv('./Qlist_4.csv')
conf = SparkConf().setMaster("local[*]").setAppName("Qlist_4")
sc = SparkContext(conf=conf)

# 使用toDF()方法需要from pyspark.sql import SQLContext。toDF()方法需要一个SQLContext，并且在创建DataFrame时，SQLContext需要存在
SQLsc = SQLContext(sc)

rdd = sc.textFile('./Qlist_4.csv').zipWithIndex().filter(lambda x: x[1] > 4).map(lambda x: x[0].split(','))
schema_rdd = rdd.first()
main_rdd = rdd.filter(lambda x: x != schema_rdd)
# print(rdd.collect())
# print(schema_rdd)
# print(main_rdd.collect())
df = main_rdd.toDF(schema_rdd)
df.show()
# Spark 实现wordcount
# 注意，编写spark程序，要求程序必须要有入口，不能直接编写python代码

# 如果快速编写程序入口：快捷键，main，然后有提示后回车
from pyspark import SparkContext,SparkConf
if __name__ == '__main__':

    # 1. 创建sc(sparkContext) 对象
    # 如何能快速的拿到一个返回值： ctrl + alt + v
    conf = SparkConf().setMaster("local[*]").setAppName("wc01")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # 2. 读取文件:尝试读取本地文件
    # 读取本地文件的协议: file:///
    # 读取hdfs文件协议: hdfs://node1:8020
    rdd = sc.textFile("file:///C:/Projects/PycharmProjects/pythonProject2/_01_pyspark_base/data/word.txt")

    # 3. 将读取到每一行数据，进行切割，得到一个大列表，放置每个单词
    rdd2 = rdd.flatMap(lambda line: line.split(", "))

    # 4. 对每一个单词转换成(单词,1)
    rdd_map = rdd2.map(lambda word: (word, 1))

    # 5. 相同的key，对于的value聚合
    key = rdd_map.reduceByKey(lambda a, b : a + b)

    # 6. 排序
    #    方式一： sortBy + take()
    #    sortBy： 用于排序，根据传入的规则进行排序操作
    #    take： 获取N个数据
    sort_key = key.sortBy(lambda x: x[1],ascending=False)

    print(sort_key.take(2))

    print('='*30)
    #    方式二： sortByKey
    #    sortByKey: 根据key进行排序，默认升序排序
    sort_by_key = key.map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))
    print(sort_by_key.take(2))

    print('='*30)
    #    方式三： top
    #    top: 直接对结果进行排序处理，只能进行降序排序，可选：定义排序规则
    print(key.top(3,lambda x: x[1]))
    # 6. 关闭资源
    sc.stop()



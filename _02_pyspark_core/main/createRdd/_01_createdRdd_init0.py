from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    # 1. 构建SparkContext对象
    # local[*]: * 表示当前环境中的cpu是几核的 *， 就表示运行多少个线程
    spConf = SparkConf().setMaster("local[*]").setAppName("init0")
    sc = SparkContext(conf=spConf)

    # 2. 通过paralleleize 获取RDD对象
    rdd = sc.parallelize(range(10))

    # 3. 打印这个RDD对象中的数据
    # getNumPartitions(),是用于获取当前这个rdd有多少个分区数
    # glom(): 获取每一个分区的数据
    # print(rdd.collect())
    print(rdd.getNumPartitions())
    print(rdd.glom().collect())
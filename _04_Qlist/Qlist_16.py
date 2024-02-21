from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, collect_set

spark = SparkSession.builder.appName('test_16').getOrCreate()
data = [('Genece', 2, 75000),
        ('xxx1', 2, 80000),
        ('xxx2', 2, 80000),
        ('Tarvares', 2, 70000),
        ('Marlania', 4, 70000),
        ('Briana', 4, 85000),
        ('xxx4', 4, 55000),
        ('xxx5', 4, 55000),
        ('Lakken', 5, 60000),
        ('Latoynia', 5, 65000)]
schema = "emp_name string,dept_id int,salary int"
df = spark.createDataFrame(data, schema)
# df1 = df.groupBy('dept_id').agg(max("salary").alias('max_sal'),min("salary").alias('min_sal'))
# df2 = df.join(df1,df.dept_id == df1.dept_id,'inner')
# df3 = df2.filter((col("salary") == col("max_sal")) | (col("salary") == col("min_sal")))
# df4 = df3.groupby('salary').agg(collect_set('emp_name').alias('emp_name'))
df4 = df.groupby('salary').agg(collect_set('emp_name').alias('emp_name'))
df4.show()
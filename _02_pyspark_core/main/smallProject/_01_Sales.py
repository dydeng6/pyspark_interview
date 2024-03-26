from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import month,year,quarter
spark = SparkSession.builder.appName('sales_engine').getOrCreate()

schema = StructType([
    StructField('product_id', IntegerType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_date', DateType(), True),
    StructField('location', StringType(), True),
    StructField('source_order', StringType(), True)
])
sales_df = spark.read.option("header",False).schema(schema).csv('../../../_02_pyspark_core/data/sales.csv.txt')

sales_df = sales_df.withColumn('order_year',year('order_date'))
sales_df = sales_df.withColumn('order_month',month('order_date'))
sales_df = sales_df.withColumn('order_quarter',quarter('order_date'))
# sales_df.show()

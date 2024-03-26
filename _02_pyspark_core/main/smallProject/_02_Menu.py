from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import month,year,quarter
spark = SparkSession.builder.appName('sales_engine').getOrCreate()
schema = StructType([
    StructField('product_id', IntegerType(), True),
    StructField('product_name', StringType(), True),
    StructField('price', StringType(), True)
])
menu_df = spark.read.option("header",False).schema(schema).csv('../../../_02_pyspark_core/data/menu.csv.txt')
# menu_df.show()
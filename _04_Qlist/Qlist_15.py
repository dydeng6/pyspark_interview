from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col

spark = SparkSession.builder.appName('test_15').getOrCreate()

data=[(1,5),(2,6),(3,5),(3,6),(1,6)]
schema="customer_id int,product_key int"
customer_df=spark.createDataFrame(data,schema)
# customer_df.show()

data=[(5,),(6,)]
schema="product_key int"
product_df=spark.createDataFrame(data,schema)
# product_df.show()

df_product = product_df.agg(countDistinct(col('product_key')).alias('cn_products'))
df_customer = customer_df.groupBy(col('customer_id')).agg(countDistinct(col('product_key')).alias('cn_products'))

df = df_product.join(df_customer, df_product.cn_products == df_customer.cn_products,'inner').select(df_customer.customer_id)
df.show()
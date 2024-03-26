from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count, countDistinct

from _01_Sales import sales_df
from _02_Menu import menu_df
spark = SparkSession.builder.appName('sales_engine').getOrCreate()
total_amount_spent = sales_df.join(menu_df,'product_id',how='left').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id')
total_amount_product = sales_df.join(menu_df,'product_id',how='left').groupBy('product_name').agg({'price':'sum'}).orderBy('product_name')
# total_amount_product.show()

# how many times each product purchased
most_df = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name')
           .agg(count('product_id').alias('product_count'))
           .orderBy('product_count',ascending=0)
           .drop('product_id'))
# most_df.show()

# Top five ordered items
top5_df = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name')
           .agg(count('product_id').alias('product_count'))
           .orderBy('product_count',ascending=0)
           .drop('product_id')
           .limit(5)
           )
# top5_df.show()

# frequency of customer visited to restaurant
fs_df = (sales_df.filter(sales_df.source_order =='Restaurant').groupBy('customer_id').agg(countDistinct('order_date')))
fs_df.show()
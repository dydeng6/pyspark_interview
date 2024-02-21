from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test_14').getOrCreate()
data=[(1,'Sagar'),(2,'Alex'),(3,'John'),(4,'Kim')]
schema="Customer_ID int, Customer_Name string"
df_customer=spark.createDataFrame(data,schema)

data=[(1,4),(3,2)]
schema="Order_ID int, Customer_ID int"
df_order=spark.createDataFrame(data,schema)

df = df_customer.join(df_order,df_customer.Customer_ID == df_order.Customer_ID,'left').filter(df_order.Customer_ID.isNull()).select(df_customer.Customer_ID)
df.show()
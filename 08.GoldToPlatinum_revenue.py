#Reporting layer

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from utils import read_config_from_s3, read_parquet, write_data_parquet_fs

spark=SparkSession.builder.appName("GOLDtoREPORTING_Revenue").getOrCreate()

config_file_s3 = "s3://glueprojnikhil/config/03.CommonConfig.json"
config_data=read_config_from_s3(config_file_s3)

currentdate = datetime.datetime.now().strftime("%Y-%m-%d")

# Access parameters from the config data
platinum_layer_path = config_data.get("platinum_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
revenue_tbl = config_data.get("revenue_tbl", "")


print("############################################################################")
print(platinum_layer_path)
print(gold_layer_path)
print(revenue_tbl)



#read data 

df_sb=read_parquet(spark,gold_layer_path+'subscriber_details')

df_sb.createOrReplaceTempView("subscriber")

revenue_report=spark.sql("""
					select 
					SD.country as Country,
					count(SD.subscriberid) AS total_subscriber,
					sum(coalesce(pre_amount,pos_amount,0)) as total_revenue
					FROM subscriber SD
					group by SD.country,Active_flag having SD.Active_flag='A'
					""")


revenue_report.show()

#write data 

write_data_parquet_fs(spark, revenue_report, platinum_layer_path+revenue_tbl)
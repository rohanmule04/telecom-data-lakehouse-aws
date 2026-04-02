from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from utils import read_config_from_s3, read_csv, write_data_parquet_fs


spark=SparkSession.builder.appName("BronzeToSilver").getOrCreate()

config_file_s3 = "s3://glueprojnikhil/config/03.CommonConfig.json"
config_data=read_config_from_s3("config_file_s3")


# Access parameters from the config data
table_list = config_data.get("tables", [])
bronze_layer_path = config_data.get("bronze_layer_path", "")
silver_layer_path = config_data.get("silver_layer_path", "")


print("##############################################################################")
print(table_list)
print(bronze_layer_path)
print(silver_layer_path)



for table in table_list:
    print("Data Load Started for ",table)
    df=read_csv(spark, bronze_layer_path+table)
    df.show()
    write_data_parquet_fs(spark, df, silver_layer_path+table)

print("********Job Successfully Completed**********")
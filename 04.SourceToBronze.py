from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json 
from utils import read_config_from_s3, read_data_from_rdbms, write_data_fs

spark=SparkSession.builder.appName("SourceToBronze").getOrCreate()

# Read config from S3
config_file_s3 = "s3://glueprojnikhil/config/03.CommonConfig.json"
config_data = read_config_from_s3(config_file_s3)
#s3://glueprojnikhil/
#s3://glueprojnikhil/config/03.CommonConfig.json


table_list = config_data.get("tables", [])
host = config_data.get("host", "")
username = config_data.get("username", "")
pwd = config_data.get("pwd", "")
driver = config_data.get("driver", "")
bronze_layer_path = config_data.get("bronze_layer_path", "")

print("############################################################################")
print(table_list)
print(host)
print(username)
print(pwd)
print(driver)
print(bronze_layer_path)


"""
#read data function from RDBMS
def read_data_from_rdbms(spark,host,username,pwd,driver,table_name):
    df=spark.read.format("JDBC").option("url",host).option("user", username).option("password", pwd)\
        .option("driver", driver).option("dbtable", table_name).load()
    return df

def write_data_fs(spark,df,path,delim=',',header="true"):
    df.write.format("CSV").option("delimiter",delim).option("header",header).save(path)
    print("Data Successfully return in FS")"""

for table in table_list:
    print("Data Load Started for ",table)
    df=read_data_from_rdbms(spark, host, username, pwd, driver, table)
    df.show()
    write_data_fs(spark, df, bronze_layer_path+table)

print("********Job Successfully Completed**********")
#processing layer
#preprocessing to processed layer 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from utils import read_config_from_s3, read_parquet, write_data_parquet_fs


spark=SparkSession.builder.appName("SilverToGold_CmpDtl").getOrCreate()


config_file_s3 = "s3://glueprojnikhil/config/03.CommonConfig.json"
config_data=read_config_from_s3(config_file_s3)

# Access parameters from the config data
table_list = config_data.get("tables", [])
silver_layer_path = config_data.get("silver_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
cmp_dtl_tgt_tbl = config_data.get("cmp_dtl_tgt_tbl", "")


print("&&&&&&&&&&&&&&&&&&################################***************************")
print(table_list)
print(gold_layer_path)
print(silver_layer_path)


#read from preprocessing transoform as per logic and store in processed 




#read data 

df_sb=read_parquet(spark,silver_layer_path+'subscriber')
df_cm=read_parquet(spark,silver_layer_path+'complaint')


#read from processed transform and store in report s3 

#DSL Approch
df_sbb=df_sb.selectExpr("sid","name")
c_test=df_cm.join(df_sbb, "sid", how="inner")

c_test.show(5)

#Give Alias
res_cmd=c_test.selectExpr("sid as subscriberId", "name as subscribername","cmp_id as complaintId","regarding as complaintReg","descr as description","sys_cre_date as com_cre_date","sys_upd_date as com_upd_date","status as status")

res_cmd.show(5)

write_data_parquet_fs(spark, res_cmd, gold_layer_path+cmp_dtl_tgt_tbl)



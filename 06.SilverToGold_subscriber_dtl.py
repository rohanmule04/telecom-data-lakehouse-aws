#processing layer
#preprocessing to processed layer 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from utils import read_config_from_s3, read_parquet, write_data_parquet_fs



spark=SparkSession.builder.appName("SilverToGold_SubDtl").getOrCreate()

config_file_s3 = "s3://glueprojnikhil/config/03.CommonConfig.json"
config_data=read_config_from_s3(config_file_s3)




# Access parameters from the config data
table_list = config_data.get("tables", [])
silver_layer_path = config_data.get("silver_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
sub_dtl_tgt_tbl = config_data.get("sub_dtl_tgt_tbl", "")

print(table_list)
print(gold_layer_path)
print(silver_layer_path)
print(sub_dtl_tgt_tbl)


#read data 

df_sb=read_parquet(spark,silver_layer_path+'subscriber')
df_ad=read_parquet(spark,silver_layer_path+'address')
df_ct=read_parquet(spark,silver_layer_path+'city')
df_cn=read_parquet(spark,silver_layer_path+'country')
df_ppr=read_parquet(spark,silver_layer_path+'plan_postpaid')
df_ppo=read_parquet(spark,silver_layer_path+'plan_prepaid')

#Apply Logic
test=df_sb.join(df_ad,"add_id", how="left").join(df_ct,"ct_id", how="left").join(df_cn, "cn_id", how="left")
test1=test.join(df_ppr, df_ppr.plan_id == test.prepaid_plan_id, how= "left").drop("add_id").drop("ct_id").drop("cn_id").drop("plan_id").withColumnRenamed("plan_desc","pre_plan_desc").withColumnRenamed("amount","pre_amount")

test2=test1.join(df_ppo, df_ppo.plan_id == test1.postpaid_plan_id, how= "left").drop("plan_id").withColumnRenamed("plan_desc","pos_plan_desc").withColumnRenamed("amount","pos_amount")



res=test2.selectExpr("sid as subscriberid", "name as subscribername","mob as contactnumber","email as emailid","street as address","ct_name as city","cn_name as country","sys_cre_date as create_date","sys_upd_date as update_date","active_flag as active_flag","pre_plan_desc as prepaid_desc","pre_amount as pre_amount","pos_plan_desc as postpaid_desc","pos_amount as pos_amount")

res.show(5)

write_data_parquet_fs(spark, res, gold_layer_path+sub_dtl_tgt_tbl)

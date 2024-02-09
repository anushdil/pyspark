#*****************************************************************************************************
#*****************************************************************************************************
#   Author :Anusha Pathiranage
#   Date : 2023-12-04
#   Period : Daily
#   Program : New Oracle mCRM data loading to Bigdata
#*****************************************************************************************************
#*****************************************************************************************************

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


spark = SparkSession.builder.master("yarn").appName("mCRM_Loading").enableHiveSupport().getOrCreate()

import sys

import datetime

# Get today's date
today = datetime.date.today()

# Subtract one day to get yesterday's date
yesterday = today - datetime.timedelta(days=1)

# Format the date as string - DWH 
yday_dwh = yesterday.strftime('%d-%b-%y')

# Format the date as string -  Bigdata 
yday_bd = yesterday.strftime('%Y-%m-%d')

d1 = "2021-01-01"
d2 = "2021-01-01"

# Table Exsistence Check
def table_check(table_name):
    try:
        df = spark.sql("""select * from {}""".format(table_name))
    except Exception as e:
        return False 
    rows = df.count()
    if(rows>1):
        return True
    else :
        return False
		
# Oracle Table  MCRM_DEV.MBL_USER_GROUP_MAPPING
# Define schema 

custom_schema = StructType([
	StructField("ID",StringType(), True),
	StructField("CREATED_BY",StringType(), True),
	StructField("CREATED_ON",StringType(), True),
	StructField("GROUP_ID",StringType(), True),
	StructField("MODIFIED_BY",StringType(), True),
	StructField("MODIFIED_ON",StringType(), True),
	StructField("USER_ID",StringType(), True),
	StructField("USER_STATUS",StringType(), True),
	StructField("PRIMARY_FLAG",StringType(), True),
	StructField("MBL_USER_GROUP_MAPPING",StringType(), True),
	StructField("FK_EMP_NO",StringType(), True),
	StructField("EMP_NO",StringType(), True),
	StructField("dateval",DateType(), True)  
])


query = """(
	With G1 as ( 
		Select ID, CREATED_BY, CREATED_ON,GROUP_ID, MODIFIED_BY, MODIFIED_ON, USER_ID, USER_STATUS, PRIMARY_FLAG, MBL_USER_GROUP_MAPPING, FK_EMP_NO, EMP_NO ,to_char(CREATED_ON,'YYYY-MM-DD') as dateval from MCRM_DEV.MBL_USER_GROUP_MAPPING where to_char(CREATED_ON,'YYYY-MM-DD') >= '{}' 
	),
	G2 as (
		
		Select ID, CREATED_BY, CREATED_ON,GROUP_ID, MODIFIED_BY, MODIFIED_ON, USER_ID, USER_STATUS, PRIMARY_FLAG, MBL_USER_GROUP_MAPPING, FK_EMP_NO, EMP_NO ,to_char(MODIFIED_ON,'YYYY-MM-DD') as dateval from  MCRM_DEV.MBL_USER_GROUP_MAPPING where to_char(MODIFIED_ON,'YYYY-MM-DD') >= '{}' 
	),
	G3 as (
		select * from G1 union all 
		select * from G2 
	)
	select * from G3 
)""".format(d1,d1)

df_mbl_um_user_grp_map = spark.read \
	.format("jdbc") \
	.option("url", "jdbc:oracle:thin:@oracle_service_link") \
	.option("driver", "oracle.jdbc.OracleDriver") \
	.option("dbtable", query) \
	.option("user", "user_name") \
	.option("password", "pw") \
    .load()



for field in custom_schema.fields:
    df_mbl_um_user_grp_map = df_mbl_um_user_grp_map.withColumn(field.name, f.col(field.name).cast(field.dataType))


df_mbl_um_user_grp_map.printSchema()
df_mbl_um_user_grp_map.createOrReplaceTempView("MBL_USER_GRP")

# Write data with partitioning
if not table_check("com_land.l_mcrm_mbl_user_grp_mapping") :
    # Coalesce to reduce the number of partitions
    coalesced_df = df_mbl_um_user_grp_map.coalesce(5)
    coalesced_df.write.mode('append') \
        .partitionBy('dateval') \
        .saveAsTable("com_land.l_mcrm_mbl_user_grp_mapping",format='parquet',path="table_path/l_mcrm_mbl_user_grp_mapping")
else:
    print("********************************BREAK****************************************************")






		
# Oracle Table  MCRM_DEV.MBL_USER_SKILL_MAPPING
# Define schema 

custom_schema = StructType([
	StructField("ID",StringType(), True),
	StructField("CREATED_BY",StringType(), True),
	StructField("CREATED_ON",StringType(), True),
	StructField("MODIFIED_BY",StringType(), True),
	StructField("MODIFIED_ON",StringType(), True),
	StructField("SKILL_ID",StringType(), True),
	StructField("USER_ID",StringType(), True),
	StructField("USER_STATUS",StringType(), True),
	StructField("dateval",DateType(), True)  
])


query = """(
	With G1 as ( 
		Select ID, CREATED_BY, CREATED_ON, MODIFIED_BY, MODIFIED_ON, SKILL_ID,USER_ID, USER_STATUS ,to_char(CREATED_ON,'YYYY-MM-DD') as dateval from MCRM_DEV.MBL_USER_SKILL_MAPPING where to_char(CREATED_ON,'YYYY-MM-DD') >= '{}' 
	),
	G2 as (
		
		Select ID, CREATED_BY, CREATED_ON, MODIFIED_BY, MODIFIED_ON, SKILL_ID,USER_ID, USER_STATUS ,to_char(MODIFIED_ON,'YYYY-MM-DD') as dateval from  MCRM_DEV.MBL_USER_SKILL_MAPPING where to_char(MODIFIED_ON,'YYYY-MM-DD') >= '{}' 
	),
	G3 as (
		select * from G1 union all 
		select * from G2 
	)
	select * from G3 
)""".format(d1,d1)

df_mbl_um_user_skill_map = spark.read \
	.format("jdbc") \
	.option("url", "jdbc:oracle:thin:@service_link") \
	.option("driver", "oracle.jdbc.OracleDriver") \
	.option("dbtable", query) \
	.option("user", "user_name") \
	.option("password", "pw") \
    .load()



for field in custom_schema.fields:
    df_mbl_um_user_skill_map = df_mbl_um_user_skill_map.withColumn(field.name, f.col(field.name).cast(field.dataType))


df_mbl_um_user_skill_map.printSchema()
df_mbl_um_user_skill_map.createOrReplaceTempView("MBL_USER_SKILL")

# Write data with partitioning
if not table_check("com_land.l_mcrm_mbl_user_skill_mapping") :
    # Coalesce to reduce the number of partitions
    coalesced_df =  df_mbl_um_user_skill_map.coalesce(5)
    coalesced_df.write.mode('append') \
        .partitionBy('dateval') \
        .saveAsTable("com_land.l_mcrm_mbl_user_skill_mapping",format='parquet',path="tb_path/l_mcrm_mbl_user_skill_mapping")
else:
    print("********************************BREAK****************************************************")



spark.stop()
exit


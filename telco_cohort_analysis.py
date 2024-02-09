

#*****************************************************************************************************
#*****************************************************************************************************
#   Author : Anusha Pathiranage
#   Date : 2023-11-25
#   Period : one time
#   Program : Cohort Analysis for telco prepaid customers
#*****************************************************************************************************
#*****************************************************************************************************

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType 
from pyspark.sql import Row 
from pyspark.sql.functions import * 

spark = SparkSession.builder.master("yarn").appName("cohort_app").enableHiveSupport().getOrCreate()


from datetime import datetime, timedelta

# Get the current date
current_date = datetime.now()

# Calculate the first day of the current month
first_day_of_current_month = current_date.replace(day=1)

# Calculate the last day of the last month
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)



# Format the date as string -  Bigdata 
sdate = '2023-08-01' 
edate = last_day_of_last_month.strftime('%Y-%m-%d')


mon_df_recharge = spark.sql("""WITH G1 as (SELECT mobile_no,min(connected_date) as connected_date  , substr(min(connected_date) , 1 ,7) as dateval
                                  
                              FROM ext_rdbs.mobis_cus_profiles
                              WHERE connected_date >= '{}'  
                              AND   connected_date <='{}' 
                              AND   connection_type = 'PRE'  
                             
                       
group by 	mobile_no					  ) , 
	
G2  as (

select G1.*  from G1 left join (
select G1.mobile_no ,count(*) as deact_count  
from ext_rdbs.mobis_cus_profiles x Right JOIN G1   
on x.mobile_no =G1.mobile_no   
where x.connected_date<G1.connected_date 
group by G1.mobile_no )  x 
on G1.mobile_no=x.mobile_no  
where deact_count is null  ) ,

 
G3 as (select G2.mobile_no,G2.dateval as first_act_month,sum(a.recharge_amt/100) as recharge , substr(a.dateval,1,7) as month_val
from G2 join ocs_cdr_data.cdr_vou a
where substr(G2.mobile_no,2,9)=substr(a.pri_identity,3,9)
and a.dateval>='{}'
and a.dateval<='{}'
and a.dateval >=G2.connected_date
group by G2.mobile_no  ,substr(a.dateval,1,7)  ,first_act_month)

select * from G3 """.format(sdate,edate,sdate,edate))

mon_df_recharge.printSchema()
mon_df_recharge.createOrReplaceTempView("prepaid_recharge")


pivot_df_recharge = mon_df_recharge.groupBy("first_act_month").pivot("month_val").sum("recharge").fillna(0)

sorted_df_recharge = pivot_df_recharge.orderBy(col("first_act_month"))





mon_df_bo = spark.sql("""WITH G1 as (SELECT mobile_no,min(connected_date) as connected_date  , substr(min(connected_date) , 1 ,7) as dateval
                                  
                              FROM ext_rdbs.mobis_cus_profiles
                              WHERE connected_date >= '{}'  
                              AND   connected_date <='{}' 
                              AND   connection_type = 'PRE'  
                             
                       
group by 	mobile_no					  ) , 
	
G2  as (

select G1.*  from G1 left join (
select G1.mobile_no ,count(*) as deact_count  
from ext_rdbs.mobis_cus_profiles x Right JOIN G1   
on x.mobile_no =G1.mobile_no   
where x.connected_date<G1.connected_date 
group by G1.mobile_no )  x 
on G1.mobile_no=x.mobile_no  
where deact_count is null  ) ,

 
G3 as (
select k.first_act_month ,k.dateval as monthval  , sum(k.arpu) as arpu_amt from (

select a.callingpartynumber,G2.mobile_no ,substr(a.dateval,1,7) as dateval , G2.dateval as first_act_month ,sum(a.debit_from_prepaid/100) as arpu
from ocs_cdr_data.cdr_voice a, G2 
where substr(a.callingpartynumber,3,9)=substr(G2.mobile_no,2,9)
and a.dateval>='{}'
and a.dateval<='{}'
and a.dateval >=G2.connected_date
and a.serviceflow='1'
group by  a.callingpartynumber,G2.mobile_no  ,substr(a.dateval,1,7) ,first_act_month
union all
select a.callingpartynumber,G2.mobile_no ,substr(a.dateval,1,7) as dateval ,G2.dateval as first_act_month,sum(a.debit_from_prepaid/100) as arpu
from ocs_cdr_data.cdr_data a, G2 
where substr(a.callingpartynumber,3,9)=substr(G2.mobile_no,2,9)
and a.dateval>='{}'
and a.dateval<='{}'
and a.dateval >=G2.connected_date
group by  a.callingpartynumber,G2.mobile_no,substr(a.dateval,1,7),first_act_month) k
group by k.first_act_month ,monthval)


select * from G3""".format(sdate,edate,sdate,edate))

mon_df_bo.printSchema()
mon_df_bo.createOrReplaceTempView("prepaid_burnout")





pivot_df_bo = mon_df_bo.groupBy("first_act_month").pivot("monthval").sum("arpu_amt").fillna(0)

sorted_df_bo = pivot_df_bo.orderBy(col("first_act_month"))






mon_df_active = spark.sql("""WITH G1 as (SELECT mobile_no,min(connected_date) as connected_date  , substr(min(connected_date) , 1 ,7) as dateval
                                  
                              FROM ext_rdbs.mobis_cus_profiles
                              WHERE connected_date >= '{}'  
                              AND   connected_date <='{}' 
                              AND   connection_type = 'PRE'  
                             
                       
group by 	mobile_no					  ) , 
	
G2  as (

select G1.*  from G1 left join (
select G1.mobile_no ,count(*) as deact_count  
from ext_rdbs.mobis_cus_profiles x Right JOIN G1   
on x.mobile_no =G1.mobile_no   
where x.connected_date<G1.connected_date 
group by G1.mobile_no )  x 
on G1.mobile_no=x.mobile_no  
where deact_count is null  ) ,

 
G3 as (
select k.first_act_month ,k.dateval as monthval  , count(distinct k.mobile_no) as cnt from (

select a.pri_identity,G2.mobile_no ,substr(a.dateval,1,7) as dateval , G2.dateval as first_act_month 
from ocs_cdr_data.cdr_voice a, G2 
where substr(a.pri_identity,3,9)=substr(G2.mobile_no,2,9)
and a.dateval>='{}'
and a.dateval<='{}'
and a.dateval >=G2.connected_date
group by  a.pri_identity,G2.mobile_no  ,substr(a.dateval,1,7) ,first_act_month
union all
select a.pri_identity,G2.mobile_no ,substr(a.dateval,1,7) as dateval ,G2.dateval as first_act_month
from ocs_cdr_data.cdr_data a, G2 
where substr(a.pri_identity,3,9)=substr(G2.mobile_no,2,9)
and a.dateval>='{}'
and a.dateval<='{}'
and a.dateval >=G2.connected_date
group by  a.pri_identity,G2.mobile_no,substr(a.dateval,1,7),first_act_month) k
group by k.first_act_month ,monthval)


select * from G3""".format(sdate,edate,sdate,edate))

mon_df_active.printSchema()
mon_df_active.createOrReplaceTempView("active_count")


pivot_df_active = mon_df_active.groupBy("first_act_month").pivot("monthval").sum("cnt").fillna(0)

sorted_df_active = pivot_df_active.orderBy(col("first_act_month"))


df1 = sorted_df_recharge.toPandas() 
df2 = sorted_df_bo.toPandas() 
df3 = sorted_df_active.toPandas() 
 
# Specify the Excel file path
excel_file_path = '/FILEPATH/anushag/prepaid_cohort_analysis.xlsx'

import pandas as pd

# Create an ExcelWriter object to write to the Excel file
with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
    # Write the first DataFrame to the first sheet
    df1.to_excel(writer, sheet_name='Recharge', index=False)
    
    # Write the second DataFrame to the second sheet
    df2.to_excel(writer, sheet_name='Burnout', index=False)
	
	 
    # Write the second DataFrame to the second sheet
    df3.to_excel(writer, sheet_name='Active_count', index=False)

# Save the Excel file
writer.save()





spark.stop()

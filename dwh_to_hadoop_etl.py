
#*****************************************************************************************************
#*****************************************************************************************************
#   Author : Anusha Pathiranage
#   Date : 2023-11-25
#   Period : weekly loading
#   Program : customer classfied data ETL -Data loading to hive table
#*****************************************************************************************************
#*****************************************************************************************************

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType 
from pyspark.sql import Row 
from pyspark.sql.functions import * 
from pyspark.sql.functions import col, when
from pyspark.sql import Window
from pyspark.sql.functions import col, rank

spark = SparkSession.builder.master("yarn").appName("customerDet_ETL").enableHiveSupport().getOrCreate()


import datetime

# Get today's date
today = datetime.date.today()

# Subtract one day to get yesterday's date
yesterday = today - datetime.timedelta(days=1)

# Format the date as string -  Bigdata 
yday_bd = yesterday.strftime('%Y-%m-%d')



# extract voice users for last month
df_device=spark.sql("""  select distinct msisdn as mobile_no
from voice_tb 
where dateval>=to_date(add_months(current_timestamp(),-1))
and dateval<= to_date(current_timestamp()) 
union
select distinct msisdn as mobile_no
from data_tb
where dateval>=to_date(add_months(current_timestamp(),-1))
and dateval<= to_date(current_timestamp())   
 """)    

#extract customer profiling data and generate age,gender and device
df_customer=spark.sql("""select trim(a.mobile_no) mobile_no,trim(a.debit_led_acc_code) debit_led_acc_code,trim(a.subscriber_code) subscriber_code,
                         trim(a.connection_type) as type,trim(a.sales_account_type) as  sal_account_type,trim(a.prefered_language)  prefered_language,
					     NVL( upper(trim(a.per_district_code)) ,upper(trim(a.bill_district_code)  )      ) as district,
						 NVL(upper(trim(a.per_city) )  ,upper(trim(a.city))  )as city,
                         trim(a.social_id_code) as nic_code,
                         trim(a.social_id_no) as nic_no ,
                         case
						when ( length(social_id_no)=10 and upper(social_id_code)='NIC' and substring(social_id_no,3,5)>'500') then 'F'
						when ( length(social_id_no)=10 and upper(social_id_code)='NIC' and substring(social_id_no,3,5)<'500') then 'M'
						end as gender,
						case
						when ( length(social_id_no)=10 and upper(social_id_code)='NIC' ) then (substring(year(current_date),3,4) + (100-cast(substring(social_id_no,1,2) as int)))
						end as age ,
						b.device_type
						from cus_profiles a LEFT JOIN device_data   b
						on a.mobile_no=b.mobile_no
						where a.disconnected_on='' """ )
						  


#extract last 3 months revenue for all pre,pos users
df_arpu=spark.sql("""select t.mobile_no , sum(t.ARPU)  as bill from (
                     select concat('0',substring(a.pri_identity,3,11)) as mobile_no,sum(a.recharge_amt/100)/3 as ARPU
	                 from vou_tb a
                     where  a.dateval >= to_date(add_months(current_timestamp(),-3))
                     and a.dateval <=to_date(current_timestamp())                     
                     group by a.pri_identity
		
                     union all 
                     select b.mobile_no ,sum(b.value)/3 as ARPU
                     from bill_data a, bill_ref_data b
                     where a.bill_no=b.bill_no
                     AND a.bill_date >= to_date(add_months(current_timestamp(),-3))
                     AND a.bill_date <=to_date(current_timestamp())  
                     group by b.mobile_no ) t
                     group by t.mobile_no """)

#extract  package info
df_package=spark.sql( """select t.package ,t.mobile_no 
                         from (SELECT pos.service_pak_code as package, pos.mobile_no  FROM (
                         SELECT a.service_pak_code, a.mobile_no, RANK() OVER(PARTITION by a.mobile_no 
                         ORDER BY a.subscribed_date DESC) row_num
                         FROM pre_pack_tb a 
                         where a.terminated_date is null 
  ) pos WHERE pos.row_num = 1
                         UNION ALL 
                         SELECT pre.service_pak_code as package, pre.mobile_no FROM (
                         SELECT a.service_pak_code,a.mobile_no, RANK() OVER(PARTITION by a.mobile_no
                         ORDER BY a.subscribed_date DESC) row_num
                         FROM pos_pack_tb a
                         where a.terminated_date is null  ) pre WHERE pre.row_num =1 ) t """)


# Joined DataFrames

joined_df = df_device.join(df_customer, "mobile_no", "left_outer") \
    .join(df_arpu, "mobile_no", "left_outer") \
    .join(df_package, "mobile_no", "left_outer")
	

#selecting table columns
hive_df=joined_df.select("mobile_no","debit_led_acc_code","subscriber_code","type","sal_account_type","prefered_language","district" ,"city","nic_code","nic_no","age","gender","device_type","bill","package")
hive_df.printSchema()
hive_df.createOrReplaceTempView("customer_profile")



#extract advertise service activated users 
mob_sql = """  (select mobile_no from advert_register
              where acc_status='ACTIVE' ) """


df_mobis = spark.read.format("jdbc") \
        .option("url", "jdbc:informix-sqli:/connectionstring") \
        .option("driver", "com.informix.jdbc.IfxDriver") \
        .option("dbtable", mob_sql) \
        .option("user", "user") \
        .option("password", "pw") \
        .load()
        
        
df_mobis.printSchema()
df_mobis.createOrReplaceTempView('madvert_active_users')

# Add a new column madvet_status to hive_df with the count of occurrences in df_mobis for each mobile
combined_df = hive_df.join(df_mobis.groupBy("mobile_no").agg(count("*").alias("count")), "mobile_no", "left_outer").fillna(0)
combined_df = combined_df.withColumn("madvert_status", when(col("count") >= 1, "YES").otherwise("NO"))
combined_df.printSchema()

#selecting final output
df_with_status=combined_df.select("mobile_no","debit_led_acc_code","subscriber_code","type","sal_account_type","prefered_language","district" ,"city","nic_code","nic_no","age","gender","device_type","bill","package","madvert_status")

# since we have duplicate records due to package mapping data issue
window_spec = Window.partitionBy("mobile_no").orderBy(col("package").desc())

# Add the rank column based on the specified window specification
joined_df_ranked = df_with_status.withColumn("rank", rank().over(window_spec))

# Select rows where rank is 1
ranked_df = joined_df_ranked.filter(col("rank") == 1).drop("rank").distinct()
no_rows = ranked_df.count()

# Display or perform further actions with the result_df
ranked_df.printSchema()
ranked_df.createOrReplaceTempView("ranked_customer_profile_hadoop")

# write the df into hive table- advert_customer table
coalesced_df = ranked_df.coalesce(5)
coalesced_df.write.mode('overwrite') \
      .saveAsTable("advertisin_customer_tb",format='parquet',path="/filepath/advert_customer")
	

#*****************************************************************************************************
#*****************************************************************************************************
#   Task - 02 : Email Alert
#*****************************************************************************************************
#*****************************************************************************************************


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import os

# create message object instance
msg = MIMEMultipart()

# setup the parameters of the message
recipient_emails = ["email"]
msg['From'] = "sender_email"
msg['To'] = "; ".join(recipient_emails)
msg['Subject'] = "customer_classified_data  ETL"

# attach the HTML body
body = """
<html>
  <head></head>
  <body>
    <p>Hello User,<br/><br/>
       This is a System-generated Email on """+str(today)+"""<br>
    </p>
	<p style="font-size:20px;background-color:DarkBlue;" >Execution Summary</p>
    <p style="color:green;">Data Extract Day : """+str(today)+""" </p>
	<p style="color:green;"> No of rows loaded : """+str(no_rows)+"""</p>  
  </body>
</html>
"""
msg.attach(MIMEText(body,'html'))

# send the message via the server set up earlier.
server = smtplib.SMTP(host="hostmail", port=111)
server.starttls()
server.login('user', '123')
server.sendmail(msg['From'], msg['To'], msg.as_string())
server.quit()
spark.stop()
exit()



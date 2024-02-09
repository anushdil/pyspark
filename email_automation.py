#*****************************************************************************************************
#*****************************************************************************************************
#   Author : Anusha Pathiranage
#   Date : 2023-10-27
#   Period : Daily
#   Program :  stock report email automation
#*****************************************************************************************************
#*****************************************************************************************************

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

spark=SparkSession.builder.master("yarn").appName("DAILY_STOCK_BAL").getOrCreate()

import sys
import datetime
# Get today's date
today = datetime.date.today()

query = """( 
 with G1 as (select e.id ,e.mobile_no,e.f_bal,e.agent_name,f.dealer_code from (
select y.id ,y.mobile_no ,y.f_bal ,d.agent_name  from (
select x.id ,c.mobile_no ,sum(x.rld_bal) as f_bal  from (

select a.id  ,a.account_balance as rld_bal 
from AGENT_ACCOUNT a 
where a.account_state='ACTIVE'
and a.agent_level='DS' 
union all
select a.ds_code as id,sum(a.account_balance) as rld_bal  
from AGENT_ACCOUNT a
where a.agent_level='RP' 
group by a.ds_code ) x
LEFT JOIN  AGENT_ACCOUNT c
ON x.id=c.id
where c.account_state='ACTIVE'
and c.agent_level='DS'
group by x.id,c.mobile_no
) y
LEFT JOIN  AGENT_ACCOUNT_INFO d
ON y.id=d.id
) e LEFT JOIN AIMS_TO_RELOAD f
on e.id=f.ds_code
where f.dealer_code<>'-1') ,

G2 AS (
  SELECT user_name AS rh_id, dealers AS mob FROM ADMIN_ACC
  where account_level='RH'  ),

G3 as (SELECT rh_id, REGEXP_SUBSTR(mob, '[^,]+', 1, LEVEL) AS mobile_no
FROM G2
CONNECT BY REGEXP_SUBSTR(mob, '[^,]+', 1, LEVEL) IS NOT NULL
AND PRIOR rh_id= rh_id
AND PRIOR sys_guid() IS NOT NULL ) ,

G5 as (select distinct  G3.rh_id,G1.* 
from G1 left join G3 
on G1.mobile_no=G3.mobile_no ),

G6  as (select t.d_code ,
sum(case when t.product_code='60600050' then t.q_val end) as product_A,
sum(case when t.product_code='60600059' then t.q_val end) as product_B,
sum(case when t.product_code='60600100' then t.q_val end) as product_C,
sum(case when t.product_code='60600119' then t.q_val end) as product_D,
sum(case when t.product_code='60600199' then t.q_val end) as product_E
from (
select dealer_code as d_code,product_code,sum(quantity-issued_total) as q_val
 from STOCK_DEALER
 where quantity>issued_total
 and product_code in ('60600199','60600100','60600050','60600059','60600119')
 group by dealer_code,product_code
 union all
 select received_from as d_code,product_code,sum(quantity-issued_total) as q_val
 from STOCK_REP
 where quantity>issued_total
 and product_code in ('60600199','60600100','60600050','60600059','60600119') 
 group by received_from,product_code) t
 group by t.d_code ) 
  select G5.rh_id as Regional_Head_Mobile ,G5.agent_name as Dealer_Name,
 G5.mobile_no as Dealer_Mobile,G5.dealer_code as Dealer_Code,G5.f_bal as Reload ,
 G6.Rs50 , G6.Rs59 ,G6.Rs100 , G6.Rs119 , G6.Rs199
 from G5 left join G6
 on G5.id=G6.d_code
 where G5.rh_id is not null )"""


df_topup = spark.read \
	.format("jdbc") \
	.option("url", "jdbc:oracle:thin:@service_ip") \
	.option("driver", "oracle.jdbc.OracleDriver") \
	.option("dbtable", query) \
	.option("user", "user_name") \
	.option("password", "pw").load()        
        
df_topup.printSchema()
df_topup.createOrReplaceTempView('daily_stock_bal')

def pd_file_write(df,f):
    path = '/FILEPATH/' + f + '.xlsx' 
    panda_df = df.toPandas()
    panda_df.to_excel(path,header=True,index=False)
    
pd_file_write(df_topup,'Daily_Stock_Report')

#*****************************************************************************************************
#   Task - 02 : Email Alert
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
recipient_emails = ['emailid1','emailid2','emailid3']
msg['From'] = "sender_email"
msg['To'] = "; ".join(recipient_emails)
msg['Subject'] = "Daily Balance Stock Report"

# attach the HTML body
body = """
<html>
  <head></head>
  <body>
    <p>Hello User,<br/><br/>
       This is a System-generated Email on """ +str(today)+ """ <br>
    </p>
		 
  </body>
</html>
"""
msg.attach(MIMEText(body,'html'))


# Attach the XLSX file
file_path = "/FILEPATH/Daily_Stock_Report.xlsx"  
filename="Daily_Stock_Report.xlsx"
attachment = open(file_path, "rb")
part = MIMEBase("application", "octet-stream")
part.set_payload((attachment).read())
encoders.encode_base64(part)
#part.add_header("Content-Disposition", f"attachment; filename=Daily_Stock_Report.xlsx")
part.add_header("Content-Disposition", "attachment; filename={}".format(filename))
msg.attach(part)


# send the message via the server set up earlier.
server = smtplib.SMTP(host="host-email", port=port_number)
server.starttls()
server.login('username', 'password')

for recipient in recipient_emails:
    server.sendmail(msg['From'], recipient, msg.as_string())

server.quit()
spark.stop()
exit()

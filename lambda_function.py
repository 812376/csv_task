import boto3
import pandas as pd
import numpy as np
import json
import mysql.connector
import smtplib
from email.message import EmailMessage

s3_client = boto3.client('s3')
server = smtplib.SMTP_ssl("smtp.gmail.com",465)
server.login("sainath2634@gmail.com","druvanth6")
msg = EmailMessage()

my_connection=mysql.connector.connect(host="csv-task-database.c6xwf1g75u1e.ap-south-1.rds.amazonaws.com",
        user="BIadmin",
        passwd="EEZPK4163k@",
        database="csv-task-database"
)
def read_data_from_s3(event):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_client.get_object(bucket=bucket_name, key = s3_file_name)
    data = resp['Body'].read().decode('utf-8')
    data = data.split(",")
    return data


def lambda_handler(event, context):
    df = pd.read_csv(read_data_from_s3)

    df1 = df.loc[(df['Region']=="Europe")] 

    df2 = df[(df1['Order_Date'] >= '01-04-2015') & (df1['Order_Date'] <= '30-04-2001')]

    df2.sort_values(['Region','Country','Order_Date','Total_Revenue'],ascending=True)
    
    df3 = df2.to_sql(con=my_connection, name='report1',if_exists='replace',index=False)
    # report1 = df2.to_csv('report1')

    df3.to_csv(r"C:\Users\korivi sainath\Desktop\case study\report1.csv")
    print(df3.head())

    # msg.add_attachment(report1,maintype="application",subtype="csv")
    server.sendmail("sainath2634@gmail.com","sainath.appdevlpr@gmail.com","report1 has been sent")
    server.quit()

    return 
    {
        'body':"report generated successfully"
    }




from pyspark.sql import SparkSession
from spamcleanse import spamlist1
from Source3 import spamlist3
from Source3 import hamlist
from GmailReader import list
import pandas as pd
import pyspark
import os
import psycopg2
from pyspark.sql.types import IntegerType, StructType, StringType, StructField

#Declares column names
columns=["Subject", "Sender", "Recipient", "Date", "Message", "Time", "HamSpam","SourceName", "SourceLink"]

#gets data from sources and converts to pandas dataframes
source1 = pd.DataFrame(spamlist1, columns= columns)
source2 = pd.DataFrame(list, columns=columns)
source3 = pd.DataFrame(hamlist, columns=columns)
s3spam = pd.DataFrame(spamlist3, columns=columns)

#combines all data sources into one dataframe
x = pd.concat([source1,source2,source3,s3spam])

# merge all in to spark
schema = StructType([ \
    StructField("subject", StringType(), True), \
    StructField("sender", StringType(), True), \
    StructField("recipient", StringType(), True), \
    StructField("date", StringType(), True), \
    StructField("message", StringType(), True), \
    StructField("time", StringType(), True),
    StructField("hamSpam", StringType(), True), \
    StructField("sourceName", StringType(), True), \
    StructField("sourceLink", StringType(), True) \
 \
    ])

#Creates and displays spark data frame
spark = SparkSession.builder.appName('MG').getOrCreate()
data = spark.createDataFrame(x, schema)
data.show()

# convert all back to pandas

#opens connection to the postgress db
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="password")

#the query to be sent to the db to create table
create = """
Create table emails(
    primKey serial primary key,
    subject varchar(1000) not null,
    sender varchar(100) not null,
    recipient varchar(100) not null,
    date varchar(30) not null,
    message varchar(1000000),
    time varchar(30) not null,
    hamSpam varchar(30) not null,
    sourceName varchar(50) not null,
    sourceLink varchar(10000) not null

)
"""

#drops table if table already exists
drop = """
 drop table if exists emails
"""

#inserts data into table
insertinto = """
insert into emails(subject, sender, recipient, date, message, time, hamSpam, sourceName, sourceLink)
values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""



cur = conn.cursor()

#executes the drop table if already exists
cur.execute(drop)
conn.commit()

#creates the new table
cur.execute(create)
conn.commit()




#Loads the data into the table line by line
for row in x.values:
    cur.execute(insertinto,(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))
    conn.commit()



cur.close()

#tells the file dimensionTable.py to execute next
os.system("python DimensionTable.py")
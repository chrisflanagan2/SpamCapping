import os
import psycopg2

#Connects to the database
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="password")

#Makes a new table for the dimension calendar
createCal = """
Create table calendar(
    calKey serial primary key,
    dateTime varchar(50),
    Date varchar(50),
    Time varchar(50),
    Year varchar(30),
    Month varchar(30),
    Day varchar(30),
    Hour varchar(30),
    Minute varchar(30),
    Second varchar(30)
)
"""

#if dimension calendar already exists drop table
dropCal = """
 drop table if exists calendar
"""

#insert the data into table
insertCal = """
    insert into calendar(dateTime, Date, Time, Year, Month, Day, Hour, Minute,Second)
    select distinct concat(Date, concat(' ' ,Time)),Date, Time, substring(date,length(date)-3,4),substring(date,0,position('-' in date)), replace(substring(date,position('-' in date)+1,2),'-', ''),substring(time,0,3),substring(time,4,2),substring(time,7,2)
    from emails
"""

#Create a table for the sender
createSender = """
Create table sender(
    sendKey serial primary key,
    email varchar(100),
    userName varchar(100),
    site varchar(100),
    domExt varchar(100)    
)
"""

#drops sender table if already exists
dropSend = """
 drop table if exists sender
"""

#adds sender into the table
insertSend = """
    insert into sender(email, userName, site, domExt)
    VALUES (%s,%s,%s,%s)
"""

#gets senders from email table
getSender = """
SELECT DISTINCT sender
FROM emails
"""

#creates a table for the source
createSrc = """
Create table source(
    srcKey serial primary key,
    srcName varchar(50) not null,
    srcLink varchar(10000) not null
)
"""

#drop source table if already exists
dropSrc = """
 drop table if exists source
"""

#inserts data into source table
insertSrc = """
    insert into source(srcName, srcLink)
    select distinct sourceName, sourceLink
    from emails
"""

#creates fact table
createFact = """
Create table factTable(
    datePK int not null,
    sendPK int not null,
    srcPK int not null,
    recipient varchar(100),
    subject varchar(1000),
    message varchar(1000000),
    hamspam varchar(100)
)
"""

#drops fact table if already exists
dropFact = """
 drop table if exists factTable
"""

#inserts data into fact table
insertFact = """
    insert into factTable(datePK, sendPK, srcPK, recipient, subject, message, hamspam)
    select distinct calendar.calKey, sender.sendKey, src.srcKey, emails.recipient, emails.subject, emails.message, emails.hamspam
    from emails, sender, source as src, calendar
    where emails.sourcename = src.srcName
    and emails.sourcelink = src.srcLink
    and calendar.Time = emails.time
    and calendar.Date = emails.date
    and emails.sender = sender.email
"""

cur = conn.cursor()

#drops src table if already exists
cur.execute(dropSrc)
conn.commit()
#creates src table
cur.execute(createSrc)
conn.commit()
#populates src table
cur.execute(insertSrc)
conn.commit()

#drops calendar table
cur.execute(dropCal)
conn.commit()
#creates calendar table
cur.execute(createCal)
conn.commit()
#populates calendar table
cur.execute(insertCal)
conn.commit()

#drops sender table
cur.execute(dropSend)
conn.commit()
#creates sender table
cur.execute(createSender)
conn.commit()
#gets senders from emails
cur.execute(getSender)
conn.commit()
sender = cur.fetchall()

#populates sender table
for x in sender:
    begin = x[0][:x[0].find("@")]
    mid = x[0][x[0].find("@")+1:x[0].rfind(".")]
    end = x[0][x[0].rfind(".")+1:]
    cur.execute(insertSend, (x[0], begin, mid, end))
    conn.commit()

#drops fact table
cur.execute(dropFact)
conn.commit()
#creates fact table
cur.execute(createFact)
conn.commit()
#populate fact table
cur.execute(insertFact)
conn.commit()

cur.close()

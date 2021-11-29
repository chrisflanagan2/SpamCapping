import pandas as pd
import re

import psycopg2
import os
import csv
import base64
import email
from bs4 import BeautifulSoup
from datetime import datetime
from bs4.element import Comment
#import pyscopg2

#Code to convert html into text
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)


spamlist1 = []
month = []
#Getting the month numbers into the proper string format
for x in range(1,13):
    if x//10==0:
        y = "0"+str(x)
        month.append(y)
    else:
        month.append(str(x))

#goes through each 2020 month folder to collect the data
for m in month:
    path = "./Data/2020/" + m + "/"
    #print(path)

    #accesses the files from that month folder
    listdir = os.listdir(path)
    for dirname in listdir:
      with open(path + dirname,'r', errors = 'ignore') as file:
        text = file.read()

        # Cleansing for the Date attribute
        matches = re.finditer(r"^Date.*", text, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
          temp = match.group()[match.group().find(":")+2:]
          if (temp.find(",") != -1 and temp.find(":")!=-1 and temp.find("2020")!=-1):
            temp2 = temp[temp.find(",") + 2:temp.find(":") - 3]
            if len(temp2)==11:
                date = datetime.strptime(temp2, "%d %b %Y").date()
                time = temp[temp.find(":") - 2:temp.find(":") + 6]
            else:
                date = None
                time = None
          else:
            date = None
            time = None

        # Cleansing for the Sender attribute
        matches = re.finditer(r"^From:.*", text, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
            x = (match.group()[match.group().find("<")+1:match.group().find(">")])
            if x.find("@")!=-1 and x.find("From:")==-1:
              sender = x
            else:
              sender = None

        # Cleansing for the Recipient attribute
        matches = re.finditer(r"^To:.*", text, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
          #x = (match.group()[match.group().find("<") + 1:match.group().find(">")])
          if match.group().find("@") != -1 and match.group().find("From:") == -1:
            if match.group().find("<") != -1 and match.group().find(">") != -1:
              recipient = match.group()[match.group().rfind("<") + 1:match.group().find(">")]
            else:
              if match.group().find(",") == -1:
                recipient = match.group()[4:].replace(" ","")
              else:
                recipient = None
          else:
            recipient = None

        # Cleansing for the Subject attribute
        matches = re.finditer(r"^Subject:.*", text, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
            subject = (match.group()[9:])

        # Cleansing for the Content attribute
        x = (text[text.find('\n\n'):])
        if x.find('<html>') != -1:
          html = x[x.find("<html>"):x.find("</html>") + len("</html>")]
          temp = text_from_html(html)
          if len(temp) != 0:
            message = temp
          else:
            message = None
        else:
          message = None


        # if any attribute = None it does not get added to the spamlist1
        if (recipient != None and sender!= None and date != None and time != None and len(subject) != 0 and message != None):
          spamlist1.append([subject, sender.lower(), recipient.lower(), str(date.month)+"-"+str(date.day)+"-"+str(date.year), message, time, "Spam", "untroubled archieve","http://untroubled.org/spam/"])

#converts spam list one to pandas file
spam = pd.DataFrame(spamlist1, columns=["Subject", "Sender", "Recipient", "Date", "Message", "Time", "Ham/Spam", "SourceName", "SourceLink"])


#print(spam.info())
# conn = psycopg2.connect("dbname=Capping user=postgres password=Capping21")
#
# cur = conn.cursor()
# sql = "INSERT INTO emails(Subject,Sender,Recipient,EmailDate,EmailTime," \
#       "EmailMessage,Ham_Spam) VALUES(%s,%s,%s,%s,%s,%s,%s)"
#
# print(sql)
# cur.execute(sql,('hello','bob','joe','10-10-2010','00:00:00','dogs are cool','Ham'  ))
# conn.commit()
# #print(cur.fetchone())
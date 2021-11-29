import psycopg2
import pandas as pd
import re
#import py7zr
import os
import csv
#import base64
#import email
from bs4 import BeautifulSoup
from bs4.element import Comment
from datetime import datetime

#Code to seperate the text form the html tags
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


#spam list
spamlist3 = []
path = "./Data/hamnspam/spam/"
listdir = os.listdir(path)
#Goes through each file in the path provided
for dirname in listdir:
  with open(path + dirname,'r', errors = 'ignore') as file:
    text = file.read()

    # Cleansing for the Subject attribute
    matches = re.finditer(r"^Subject:.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      subject = match.group()[9:]

    # Cleansing for the To attribute
    matches = re.finditer(r"^To:.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      if(match.group().find(',')==-1 and match.group().find("@")!=-1 and match.group().find("C:")==-1 and match.group().find("=")==-1 and match.group().find("Subject:")==-1):
        if(match.group().find("<")!=-1):
          to = match.group()[(match.group().find("<")+1):match.group().find(">")]
        else:
          to = match.group()[4:]
      else:
        to = None

    # Cleansing for the From attribute
    matches = re.finditer(r"^From:.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      if (match.group().find(',') == -1 and match.group().find("@") != -1 and match.group().find("C:") == -1 and match.group().find("=") == -1 and match.group().find("Subject:") == -1 and match.group().find("\"<")==-1):
        if (match.group().find("<") != -1):
          varfrom = match.group()[(match.group().find("<") + 1):match.group().find(">")]
        else:
          if (match.group().find("(") != -1):
            varfrom = match.group()[6:match.group().find("(") - 1]
          else:
            varfrom = match.group()[6:]
      else:
        varfrom = None

    # Cleansing for the Date attribute
    tempdate = None
    matches = re.finditer(r"^Date.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      temp = match.group()[(match.group().find(":")+2):]
      if(temp.find(":")!=-1):
        if(temp.find(",")==-1):
          if(temp.find("/")==-1):
            temp2 = temp[:temp.find(":")-3]
            if(temp2[len(temp2)-3:]!=" 02" and temp2.find("Sat")==-1):
              #datetime.datetime.strptime('May 29 2002', "%B %d %Y")
              date = datetime.strptime(temp2, "%d %b %Y").date()
              time = temp[temp.find(":")-2:temp.rfind(":")+3]
            else:
              time = None
              date = None
          else:
            time = None
            date = None
        else:
          temp2 = temp[temp.find(",")+2:temp.find(":")-3]
          if(temp2.find("0102")==-1 and temp2[len(temp2)-3:]!=" 02"):
            date = datetime.strptime(temp2, "%d %b %Y").date()
            time = temp[temp.find(":")-2:temp.rfind(":")+3]
          else:
            time = None
            date = None
      else:
        time = None
        date = None


    # Cleansing for the Message attribute
    ogMessage = text[text.find("\n\n")+2:]
    #print(len(ogMessage))
    if(ogMessage.find("<HTML>")!=-1):
      message = text_from_html(ogMessage)
    else:
      message = ogMessage

    #if any of the attributes = None then it does not get added to the spamlist3
    if(to!=None and varfrom!=None and date!=None and time!=None and len(subject)!=0 and len(message)!=0):
      spamlist3.append([subject,varfrom.lower(),to.lower(),str(date.month)+"-"+str(date.day)+"-"+str(date.year),message,time,"Spam", "kaggle Email","https://www.kaggle.com/veleon/ham-and-spam-dataset"])



#turns spamlist3 into a pandas dataframe
spam = pd.DataFrame(spamlist3, columns = ["Subject","Sender","Recipient","Date","Message", "Time","Ham/Spam", "SourceName", "SourceLink"])

#print(spam["Date"])


hamlist = []
path = "./Data/hamnspam/ham/"
listdir = os.listdir(path)
#goes through each file of the path provided
for dirname in listdir:
  with open(path + dirname,'r', errors = 'ignore') as file:
    text = file.read()

    # Cleansing for the Subject attribute
    matches = re.finditer(r"^Subject:.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      subject = match.group()[9:]

    # Cleansing for the To attribute
    matches = re.finditer(r"^To:.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      if(match.group().find(',')==-1 and match.group().find("@")!=-1 and match.group().find("C:")==-1 and match.group().find("=")==-1 and match.group().find("Subject:")==-1):
        if(match.group().find("<")!=-1):
          to = match.group()[(match.group().find("<")+1):match.group().find(">")]
        else:
          to = match.group()[4:]
      else:
        to = None

    # Cleansing for the From attribute
    matches = re.finditer(r"^From:.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      if (match.group().find(',') == -1 and match.group().find("@") != -1 and match.group().find("C:") == -1 and match.group().find("=") == -1 and match.group().find("Subject:") == -1 and match.group().find("\"<")==-1):
        if (match.group().find("<") != -1):
          varfrom = match.group()[(match.group().find("<") + 1):match.group().find(">")]
        else:
          if (match.group().find("(") != -1):
            varfrom = match.group()[6:match.group().find("(") - 1]
          else:
            if (match.group().find("[") != -1):
              varfrom = match.group()[6:match.group().find("[") - 1]
            else:
              varfrom = match.group()[6:]

      else:
        varfrom = None

    # Cleansing for the Date attribute
    matches = re.finditer(r"^Date.*", text, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
      temp = match.group()[(match.group().find(":")+2):]
      if (temp.find(":") != -1):
        if(temp.find(",")!=-1):
          temp2=temp[temp.find(",")+2:temp.find(":")-3]
          date = datetime.strptime(temp2, "%d %b %Y").date()
          time=temp[temp.find(":")-2:temp.find(":")+6]
        else:
          if(temp.find("T")==-1 and temp.find(";")==-1):
            temp2=temp[:temp.find(":")-3]
            date = datetime.strptime(temp2, "%d %b %Y").date()
            time=temp[temp.find(":")-2:temp.find(":")+6]
      else:
        time = None
        date = None


    # Cleansing for the Message attribute
    ogMessage = text[text.find("\n\n")+2:]
    #print(len(ogMessage))
    if(ogMessage.find("<HTML>")!=-1):
      message = text_from_html(ogMessage)
    else:
      message = ogMessage

    #if any of the attributes = None then it will not be added to the hamlist
    if(to!=None and varfrom!=None and date!=None and time!=None and len(subject)!=0 and len(message)!=0):
      hamlist.append([subject,varfrom.lower(),to.lower(),str(date.month)+"-"+str(date.day)+"-"+str(date.year),message,time,"Ham","kaggle Email","https://www.kaggle.com/veleon/ham-and-spam-dataset"])

#converts hamlist into a pandas dataframe
ham = pd.DataFrame(hamlist, columns = ["Subject","Sender","Recipient","Date","Message", "Time","Ham/Spam", "SourceName", "SourceLink"])
#print(ham["Date"])
#print(spam)



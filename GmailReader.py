import pandas as pd
from datetime import datetime
import psycopg2

#loads data from csv file
df = pd.read_csv("./Data/messages.csv",
                 names = ["Subject","Sender","Recipient","Date","Nothing","Message", "Time"],header =None)

#checks all columns dont have null values in column
df = df[df['Subject'].notna()]
df = df[df['Sender'].notna()]
df = df[df['Recipient'].notna()]
df = df[df['Date'].notna()]
df = df[df['Message'].notna()]


count =0
list = []
#goes through each iteration of the pandas dataframe
for i in df.index:

    #Gets the date from the message column
    temp = df["Message"][i].replace("\"", "")
    temp2 = temp[(temp.index("Date:") + len("Date:")):(temp.index("To:") - 2)]

    #gets the time from the message column
    time = temp2[(temp2.index(" ")+1):len(temp2)]

    if time[0:2].find(":")!=-1:
        time = "0"+time

    time = datetime.strptime(time, '%I:%M %p').strftime('%H:%M:%S')

    date = df["Date"][i].replace("/","-")

    #gets the message
    if temp.find("To:")!=-1:
        to = df["Recipient"][i].replace("\"", "")
        temp2 = temp[temp.find("To:"):]
        temp3 = temp2[temp2.find("\n")+2:]
        message = temp3[temp3.find("\n")+1:]
        #message = temp[temp.find("\n\n")+2:]
    else:
        message = None
        #count+=1
        #list.append(i)
        #Figure out how to drop from table if here
        #if they contain CC: in message or row

    #gets the sender from the sender column
    if df["Sender"][i].find("@")!=-1 and df["Sender"][i].find(".")!=-1 and (df["Sender"][i].find(".co")!=-1 or df["Sender"][i].find(".edu")!=-1 or df["Sender"][i].find(".net")!=-1 or df["Sender"][i].find(".org")!=-1 or df["Sender"][i].find(".gov")!=-1):
        if df["Sender"][i].find("<")!=-1:
            if df["Sender"][i].find(">")!=-1:
                sender = df["Sender"][i][df["Sender"][i].find("<")+1: df["Sender"][i].find(">")]
            else:
                sender = df["Sender"][i][df["Sender"][i].find("<") + 1:]
            #print(sender)

        else:
            sender = df["Sender"][i]
    else:
        sender = None

    #Gets the recipient from the recipient column
    if df["Recipient"][i].find("@")!=-1 and (df["Recipient"][i].find(".co")!=-1 or df["Recipient"][i].find(".edu")!=-1 or df["Recipient"][i].find(".net")!=-1 or df["Recipient"][i].find(".org")!=-1 or df["Recipient"][i].find(".gov")!=-1):
        if df["Recipient"][i].find("<")!=-1:
            if df["Recipient"][i].find(">") != -1:
                Recipient = df["Recipient"][i][df["Recipient"][i].find("<")+1: df["Recipient"][i].find(">")]
            else:
                Recipient = df["Recipient"][i][df["Recipient"][i].find("<") + 1:]
        else:
            Recipient = df["Recipient"][i]
    else:
        Recipient = None

    #If no none values appears the record gets added to the list
    if(message!=None and sender!=None and Recipient!= None):
        list.append([df["Subject"][i],sender.lower(),Recipient.lower(),date,message,time,"Ham", "Gmail", "fakeEmail72@gmail.com"])



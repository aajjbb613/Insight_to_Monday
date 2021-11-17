# Monday and Local SQL server intagration
# Written by: Anthony Bradt 613-986-0029
# Requested by: Victoria Hurrell, Hated by: Christan Slain

import requests
import json
import pyodbc
import pandas as pd
import time

apiKey = "XXX"
apiUrl = "https://api.monday.com/v2"
headers = {"Authorization": apiKey}

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=XXX;"
                      "Database=XXX;"
                      "Trusted_Connection=yes;")

#Add SQL Read Query here: 
#SQLQR = "SELECT	stt.sttDescription as 'Status' ,ord.ordSchedShipDate ,ord.ordCustRequestDate as 'Production Completion'	,sales.ordavValue as 'Designer'	,ord.ordPONumber as 'Sales Order 3'	,cust.venCompanyName as 'Customer Name',ord.ordDescription FROM dbo.Orders ord LEFT JOIN dbo.OrderStatuses stt on stt.sttID = ord.sttID LEFT JOIN dbo.OrderAttributeValues sales on sales.ordID = ord.ordID AND sales.atbID = 64 LEFT JOIN dbo.Organizations cust on cust.venID = ord.venID WHERE ord.ordCreatedDateTime > '2019-01-01' AND cust.vencompanyName NOT LIKE 'Test Customer' AND ord.ordPONumber NOT LIKE '0000%' AND sales.ordavValue = 'Heather Tardioli' "

#Must match Desingers names with Board ID
Designers = ["Elnaz Shahrokhi"  ,"Kaitlyn North"    ,"Heather Tardioli" ,"Wael Bakr"    ,"Aviva Ben-Choreen"    ,"Janet Spencer"    ,"Karley Scrivens"  ,"Kimberly Silcox"  ,"Ola Elmaghraby"   ,"Sarah Clifford"   ,"Victoria Campbell" ,"Caroline Castrucci"   ,"Corey Laurysen"   ,"Zeina Agha", "Jinan Al-Ani"]
BoardID =   ["840778743"        ,"840784633"        ,"840780676"        ,"840788263"    ,"701886327"            ,"840782335"        ,"840785291"        ,"840786638"        ,"840787425"        ,"840792017"        ,"840789036"        ,"840785983"            ,"845011609"        ,"840791247", "840783457"]
Stats = ["Cancelled","Shipped","Completed","Available for Confirmation","Available to Schedule","Blank5","Confirmation Notification","Copy","Design Import","In Production","Invoiced","Left CP","PO Needed","Scheduled","Review for Scheduling","Service Schedulable","Ready to Ship","Ready to Ship CP","Material List Available","Nested","Left Carleton Place"]
#Hard coded Status, order matters very much, must match monday's side
print(len(Stats))

""" 
'Cancelled', 'value': '{"index":0}'
'Shipped', 'value': '{"index":1}'},
'Completed', 'value': '{"index":2}'},
'Available for Confirmation', 'value': '{"index":3}'}
'Available to Schedule', 'value': '{"index":4}'},
'Confirmation Notification', 'value': '{"index":6}'}
'Copy', 'value': '{"index":7}'}
'Design Import', 'value': '{"index":8}'},
'In Production', 'value': '{"index":9}'}
'Invoiced', 'value': '{"index":10}'},
'Left CP', 'value': '{"index":11}'},
'PO Needed', 'value': '{"index":12}'}
'Scheduled', 'value': '{"index":13}'}
'Review For Scheduling', 'value': '{"index":14}'}
"""
    
def SQLRead(Des):  #Pass Designer name to SQL, Return Full SQL read
    try:
        SQLQR = "SELECT	stt.sttDescription as 'Status' ,ord.ordSchedShipDate ,ord.ordCustRequestDate as 'Production Completion'	,sales.ordavValue as 'Designer'	,ord.ordPONumber as 'Sales Order 3'	,cust.venCompanyName as 'Customer Name',ord.ordDescription, processor.ordavValue as 'Processor', ord.ordOrderDate FROM dbo.Orders ord LEFT JOIN dbo.OrderStatuses stt on stt.sttID = ord.sttID LEFT JOIN dbo.OrderAttributeValues sales on sales.ordID = ord.ordID AND sales.atbID = 64 LEFT JOIN dbo.OrderAttributeValues processor on processor.ordID = ord.ordID AND processor.atbID = 75 LEFT JOIN dbo.Organizations cust on cust.venID = ord.venID WHERE ord.ordCreatedDateTime > DATEADD(year,-1,GETDATE()) AND cust.vencompanyName NOT LIKE 'Test Customer' AND ord.ordpoNumber NOT LIKE '%-D' AND ord.ordPONumber NOT LIKE '0000%' AND sales.ordavValue = '"
        SQLQR += Des
        SQLQR += "'"
        df = pd.read_sql(SQLQR, cnxn)
        return df
    except: #If Failed, Try again. Bandain for timeout SQL Requests
        print("SQL failed")
        SQLRead(Des)
        
def STRClean(CleanME): #Removes Extra char on Strings
    CleanME = str(CleanME)
    CleanME = CleanME[2:-2]
    return CleanME
    
def CheckSTR(ID,Data): 
    Data = str(Data)
    ID = str(ID)
    ID += '"'
    #print(ID)
    if ID in Data:
        return 1
    else:
        return 0
        
def CheckSTROld(ID,Data):
    Data = str(Data)
    ID = str(ID)
    #print(ID)
    if ID in Data:
        return 1
    else:
        return 0

def MonQuery(BID): #Takes Monday Board ID, returns that boards items  
    #'query {boards (ids: 695573207){items{name column_values{title id value}}}}'
    query = 'query {boards (ids:'
    query += BID
    query += '){items{name id column_values{title id value}}}}'
    data = {'query' : query}
    r = requests.post(url=apiUrl, json=data, headers=headers) # make request
    x = r.json()
   # print(r)
    return x

def SQLToMon(FullQ,SID,BID): #Takes SQL Data, breakes in into parts and passes it to Funtion WriteMon
    Status = FullQ['Status']  #Sperates each line item
    ShipDate = FullQ['ordSchedShipDate']
    ProdComp = FullQ['Production Completion']
    Design = FullQ['Designer']
    Item = FullQ['Customer Name']
    Descrip = FullQ['ordDescription']
    OrdDate = FullQ['ordOrderDate']
    Process = FullQ['Processor']
    Process = STRClean(Process.values) #Cleans each item 
    Status = STRClean(Status.values)
    ShipDate = DateClean(ShipDate) #Dates and STR use diffrent Clean functions
    ProdComp = DateClean(ProdComp)
    OrdDate = DateClean(OrdDate)
    Design = STRClean(Design.values)
    Item = STRClean(Item.values)
    Descrip = STRClean(Descrip.values)
    print(Item,SID,Status,Descrip,ShipDate,ProdComp,Design,BID,OrdDate,Process)
    WriteMon(Item,SID,Status,Descrip,ShipDate,ProdComp,Design,BID,OrdDate,Process)
    return 0
    
def DateClean(Date):
    Date = str(Date)
    Date = Date.split("Name:",1)[0]
    Date = Date[4:]
    Date = Date.replace(" ","")
    Date = Date.replace("\n","")
    return Date

def WriteMon(Item,SID,Status,Descrip,ShipDate,ProdComp,Design,BID,OrdDate,Process):
    IID = MakeItem(BID,Item)
    IID = CleanID(str(IID))
    print(IID)
    ChangeItemValues(BID,IID,"text_1",DoubleDump(SID))
    ChangeItemValues(BID,IID,"text",DoubleDump(Descrip))
    ChangeItemValues(BID,IID,"text7",DoubleDump(Process))
    ChangeItemValues(BID,IID,"text1",DoubleDump(Design))
    ChangeItemValues(BID,IID,"date",DateDump(ProdComp))
    ChangeItemValues(BID,IID,"date4",DateDump(ShipDate))
    ChangeItemValues(BID,IID,"date1",DateDump(OrdDate))
    print(Status)
    for z in range(len(Stats)):
        if CheckSTROld(Stats[z],Status):
            ChangeItemValues(BID,IID,"status",StatDump(z))
            
    
 #   ChangeItemValues(BID,IID,
    return 0
    
def StatDump(Value):
    VStat = '{"index":%s}'%(Value)
    VStat = json.dumps(VStat)
    return VStat
    
def DoubleDump(Value):
    Value = json.dumps(json.dumps(Value))
    return Value
    
def DateDump(Value):
    VDate = '{"date": "%s"}'%(Value)
    VDate = json.dumps(VDate)
    return VDate
    
def MakeItem(ID,Item):
    Item = str(Item)
    query = 'mutation { create_item (board_id:'
    query += ID
    query += ', group_id: "topics", item_name:"'
    query += Item
    query += '") { id } }'
    data = {'query' : query}
    r = requests.post(url=apiUrl, json=data, headers=headers) # make request
    print (r)
    return (r.json());
  #  return r

def CleanID(NotCleanID):
    print("To be Cleaned:" + NotCleanID)
    NotCleanID = NotCleanID.split("'id':",1)[1]
    NotCleanID = NotCleanID.split("}}",1)[0]
    NotCleanID = NotCleanID.replace("'","")
    NotCleanID = NotCleanID.replace(" ","")
    return NotCleanID
    
def CleanItemID(ItemID):
    ItemID = ItemID.split("'id':",1)[1]
    ItemID = ItemID.split("'column_values':",1)[0]
    ItemID = ItemID.replace("'","")
    ItemID = ItemID.replace(",","")
    ItemID = ItemID.replace(" ","")
    return ItemID
    
"""
text_1 - sales order#
text - Description
status - Production status
date4 - Sched Ship date
date - Production completion
person - Desginers
"""

def ChangeItemValues(BID,IID,CID,Value):
    try:
        print ("Flag: 0")
        query = str('mutation {change_column_value(board_id:%s, item_id:%s,column_id:"%s",value:%s){id}}'%(BID,IID,CID,Value))
        print (query)
        data = {'query' : query}
        print ("Flag: 1")
        r = requests.post(url=apiUrl, json=data, headers=headers, timeout=240) # make request
        print ("Flag: 2")
        return (r.json());
    except:
        print("failed")
        print(BID)
        print(IID)
        print(CID)
        print(Value)

"""
def FindWon(json_data):
    data_dict = dict(json_data)
    won_list = []
    if ("data" in data_dict.keys()):
        dict1 = data_dict["data"]
        if ("boards" in dict1.keys()):
            for item in dict1["boards"]:
                if ("items" in item.keys()):
                    for i in item["items"]:
                        if ("column_values" in i.keys()):
                            for ix in i["column_values"]:
                                if ("value" in ix.keys()):
                                    if ("61557-D" in ix["value"]):
                                        won_list.append(i)
    return (won_list);
print(type(MonQuery("695573207")))
print(FindWon(MonQuery("695573207")))
"""

lvals = []
def rec_won(data,key,reg,depth): #Majic Sause, This burger aint the same without it. Credits:Kyle Lawrynuik 
    flag = []
    new_flag = []
    global lvals
    if isinstance(data, dict):
        try:
            if (reg == str(data[key])):
                flag.append(depth)
        except KeyError as e:
            pass
        for element in data:
            for el in rec_won(data[element],key,reg,depth):
                flag.append(el)
    if isinstance(data, list):
        for element in data:
            for el in rec_won(element,key,reg,depth):
                flag.append((el))
    for element in flag:
        if(element>0):
            new_flag.append(element-1)
        if(element==0):
            #print(data)
            lvals.append(data)
    return new_flag

"""
input("part 0 -- Testing ")
lvals = []
data = MonQuery("695573207")
print(type(data))
#print(data)
salesOrderID = "63020"
salesOrderID = json.dumps(salesOrderID)
rec_won(data=dict(data),key="value",reg=salesOrderID,depth=2)
print(lvals)

"""


#Main 
input("Part 1 -- Add missing items to monday")
count = 0
for x in Designers: #Run through for each designer
    print(x)
   # input("Enter to continue")
    time.sleep(60)
    df = SQLRead(x) #Runs preset SQL Query for the designers name 
    print(df)       #Prints SQL Query using pandas
    print(BoardID[count]) 
    MonData = MonQuery(BoardID[count])#Takes hard coded desingers board ID and returns Monday Items 
    print(MonData)
    
    for y in df.index:#For every item returned by the SQL Query
        time.sleep(1)
        TestDF = df.loc[[y]] #Pass just one item at a time
        print(TestDF)
        SalesDF = TestDF['Sales Order 3'] #Isolate Sales Order number
        SalesID = STRClean(SalesDF.values)#Clean Sales Order number
        print(SalesID)
        
        if CheckSTR(SalesID,MonData) == 0: #Check if Sales Order number is anywhere in the monday board
            print("passing SQL to Monday")
            print(df.loc[y])
            SQLToMon(df.loc[[y]],SalesID,BoardID[count]) #Passes SQL item to be written to Monday.com 
           
        else: #Item already exist in monday
            lvals = [] #Preforms the same as SQLToMon, should be a function of its own
            SalesID = json.dumps(SalesID)
            rec_won(data=dict(MonData),key="value",reg=SalesID,depth=2)
            ItemID = CleanItemID(str(lvals))
            Status = TestDF['Status']
            Status = STRClean(Status.values)
            ProdDate = TestDF['Production Completion']
            SchedDate = TestDF['ordSchedShipDate']
            SchedDate = DateClean(SchedDate)
            ProdDate = DateClean(ProdDate)
            for z in range(len(Stats)):
                if CheckSTROld(Stats[z],Status):
                    ChangeItemValues(BoardID[count],ItemID,"status",StatDump(z))
                    ChangeItemValues(BoardID[count],ItemID,"date",DateDump(ProdDate))
                    ChangeItemValues(BoardID[count],ItemID,"date4",DateDump(SchedDate))
                    print("update")
                    print(StatDump(z))   
                  
    count += 1
    
input("Finished")

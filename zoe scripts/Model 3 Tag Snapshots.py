import pandas as pd
import pyodbc as db

#Connect SQL server
warp = db.connect ('DRIVER={SQL Server};SERVER=WDPRDRPTDB;DATABASE=SPACEXERP;')

#Define SQL Query folder path
Query_path = 'D:/SQL Scripts/ZW/'
File_path = '//filestore/SupplyChain/Tableau Datasource/Program Mangement/'

#Define function to read SQL scripts
def queryRead(filename):
    SQLScript = open(Query_path+filename,'r').read()
    return SQLScript

SQLScript_Warp = queryRead('Model 3 Tag.sql')

#Table results from query
output_warp = pd.read_sql(SQLScript_Warp,warp)

#Table results from Excel
#Ignore the index column and header by specifying the column names
output_excel = pd.read_csv(File_path+'Output_Model 3 EBOM Snapshot.csv',names=["Load_Date","PartNumber","ECOStatus","PartSystem","TQPStatus"],header=1)


#Append new data + existing data
output = output_excel.append(output_warp,ignore_index=True)

#Output the file to CSV
output.to_csv(File_path+'Output_Model 3 EBOM Snapshot.csv')






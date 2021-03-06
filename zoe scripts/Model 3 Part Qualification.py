import pandas as pd
import pyodbc as db
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#Connect SQL server
portal = db.connect('DRIVER={SQL Server};SERVER=ENTPRDDB2;DATABASE=APQP;')
warp = db.connect ('DRIVER={SQL Server};SERVER=WDPRDRPTDB;DATABASE=SPACEXERP;')

#Define SQL Query folder path
Query_path = 'D:/SQL Scripts/ZW/'
File_path = "//filestore/SupplyChain/Tableau Datasource/Program Mangement/"

#Define function to read SQL scripts
def queryRead(filename):
    SQLScript = open(Query_path+filename,'r').read()
    return SQLScript

SQLScript_portal = queryRead('For SIE Portal.sql')
SQLScript_Warp = queryRead('For SIE Warp.sql')

#Table results from query
output_portal = pd.read_sql(SQLScript_portal,portal)
output_warp = pd.read_sql(SQLScript_Warp,warp)

#Join Tables
output = pd.merge(output_warp,output_portal,on=['partnumber','suppliercode'],how='left')

#Output the file to CSV
output.to_csv(File_path+'Output_Portal+Warp.csv')






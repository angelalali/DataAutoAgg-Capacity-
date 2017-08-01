import pyodbc
import pandas as pd

cnxn = pyodbc.connect(
        driver='{SQL Server}',
        host='ENTPRDDB2',
        database='APQP',
        Trusted_Connection='yes',
    )
cursor = cnxn.cursor()

def capacity_data ():


    ### pandas has a really convenient in house fxn that will just read the sql results into a dtaframe for you! :D
    return pd.read_sql(query, cnxn)


# returned data columns include:
# PartSupplierTaskID	SupplierCode	SupplierName	SIE	SIEManager	GSM	GSMManager	PartID	PartNumber	ProgramTag
# TagID	TagName	Program	PartSupplierTagID	QuoteDate	Quoted	Other4CreateDate	Measured	Potential	Designed
# Theoretical	MeasuredStatus	DesignedStatus

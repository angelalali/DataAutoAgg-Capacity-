import os
import time
import pandas as pd
from QPVDataSQL import *
from CapacityDataSQL import *
import xlsxwriter
from pyexcelerate import Workbook


start = time.time()

CapacityData = capacity_data()
print('time elapsed: ', time.time() - start)
QPVData = qpv_data()
print('time elapsed: ', time.time() - start)

MergedDataAll = pd.merge(CapacityData, QPVData, how='left',
                         left_on=['PartNumber', 'SupplierCode'],
                         right_on=['PartNumber', 'SupplierCode'])


# outDir = 'C:/Users/yisli/Documents/landlordlady/Tesla/ad hoc projects/Capacity Data/outputs'
# outDir = 'D:/Python Scripts/YL/Capacity Data'
outDir = "//filestore/SupplyChain/Tableau Datasource/Capacity Data"
# outDir = 'D:/Python Scripts/landlordlady/Capacity Data/output'
if not os.path.exists(outDir):
    os.makedirs(outDir)
else:
    print("It already exists!")

merged_file = os.path.join(outDir, 'merged data.xlsx')
print(merged_file)
if os.path.exists(merged_file):
    os.remove(merged_file)

print('time elapsed: ', time.time() - start)
#
# def df_to_excel(df, path, sheet_name='Sheet 1'):
#     data = [df.columns.tolist(), ] + df.values.tolist()
#     wb = Workbook()
#     wb.new_sheet(sheet_name, data=data)
#     wb.save(path)
#
# df_to_excel(df=MergedDataAll, path=merged_file)

MergedDataAll.to_excel(merged_file, index=False, engine='xlsxwriter')
# MergedDataAll.to_csv(merged_file, index=False)

print('job finished!')
print('time elapsed: ', time.time() - start)


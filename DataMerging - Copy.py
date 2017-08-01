import os
import time
import pandas as pd
from QPVDataSQL import *
from CapacityDataSQL import *
# import xlsxwriter
# from pyexcelerate import Workbook


start = time.time()

CapacityData = capacity_data()
print('time elapsed: ', time.time() - start)
QPVData = qpv_data()
print('time elapsed: ', time.time() - start)
#
# print(CapacityData.columns.values)
# print(QPVData.columns.values)

# print(CapacityData['SupplierCode'][0:15])
# CapacityData['SupplierCode'] = CapacityData['SupplierCode'].astype(str)
# print('for capcity data: ', type(CapacityData['SupplierCode'][0]))
# print('for QPV data: ', type(QPVData['SupplierCode'][0]))
# CapacityData.set_index('PartNumber', inplace=True)
# QPVData.set_index('PartNumber', inplace=True)
#
# MergedDataAll = CapacityData.join(QPVData, how='left')
# MergedDataAll = pd.merge(CapacityData, QPVData, how='left', left_on='PartNumber', right_on='PartNumber')

MergedDataAll = pd.merge(CapacityData, QPVData, how='left',
                         left_on=['PartNumber', 'SupplierCode', 'TagName'],
                         right_on=['PartNumber', 'SupplierCode', 'TagName'])

# print(MergedDataAll.ix[:10,:])

# outDir = 'C:/Users/yisli/Documents/landlordlady/Tesla/ad hoc projects/Capacity Data/outputs'
# outDir = 'D:/Python Scripts/YL/Capacity Data'
# outDir = "//filestore/SupplyChain/Tableau Datasource/Capacity Data"
# outDir = 'D:/Python Scripts/landlordlady/Capacity Data/output'
outDir = "C:/Users/yisli/Desktop"
if not os.path.exists(outDir):
    os.makedirs(outDir)
else:
    print("It already exists!")

merged_file = os.path.join(outDir, 'merged data.xlsx')
print(merged_file)
if os.path.exists(merged_file):
    os.remove(merged_file)

print('time elapsed: ', time.time() - start)

# def df_to_excel(df, path, sheet_name='Sheet 1'):
#     data = [df.columns.tolist(), ] + df.values.tolist()
#     wb = Workbook()
#     wb.new_sheet(sheet_name, data=data)
#     wb.save(path)
#
# df_to_excel(df=MergedDataAll, path=merged_file)

MergedDataAll.to_excel(merged_file, index=False)
# MergedDataAll.to_csv(merged_file, index=False)

print('job finished!')
print('time elapsed: ', time.time() - start)


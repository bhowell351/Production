
import numpy as np
import time, PyUber, os, csv
import warnings
from datetime import datetime
import pandas as pd


new_row = pd.read_csv(r"C:\Users\bhowell\OneDrive - Intel Corporation\ME\STRS upload test.csv")
warnings.simplefilter("ignore")

atlas_sql = f"""
SELECT P_F_UDF_ATLAS_EQP_EXCLUDE.FACILITY,
       P_F_UDF_ATLAS_EQP_EXCLUDE.ENTITY,
       P_F_UDF_ATLAS_EQP_EXCLUDE.EXCLUDEFROMDATE,
       P_F_UDF_ATLAS_EQP_EXCLUDE.EXCLUDETODATE,
       P_F_UDF_ATLAS_EQP_EXCLUDE.LAST_UPDATE_DATE,
       P_F_UDF_ATLAS_EQP_EXCLUDE.LAST_UPDATE_USER,
       P_F_UDF_ATLAS_EQP_EXCLUDE.SECURITY_CODE,
       P_F_UDF_ATLAS_EQP_EXCLUDE.COMMENTS
FROM D1D_PROD_XEUSS.P_F_UDF_ATLAS_EQP_EXCLUDE P_F_UDF_ATLAS_EQP_EXCLUDE
"""

UE_sql = """
SELECT P_F_UE_CONFIG.FACILITY,
       P_F_UE_CONFIG.ENTITY,
       P_F_UE_CONFIG.SUBENTITY_1,
       P_F_UE_CONFIG.SUBENTITY_2,
       P_F_UE_CONFIG.SUBENTITY_3,
       P_F_UE_CONFIG.SUBENTITY_4,
       P_F_UE_CONFIG.SUBENTITY_5,
       P_F_UE_CONFIG.INCLUDE,
       P_F_UE_CONFIG.BATCH,
       P_F_UE_CONFIG.BATCH_TYPE,
       P_F_UE_CONFIG.HISTORY,
       P_F_UE_CONFIG.SOURCE,
       P_F_UE_CONFIG.WIP_THRESHOLD,
       P_F_UE_CONFIG.OUTPUT_INCLUDE,
       P_F_UE_CONFIG.INV_TYPE,
       P_F_UE_CONFIG.EU_TARGET,
       P_F_UE_CONFIG.GROUPING,
       P_F_UE_CONFIG.LINK,
       P_F_UE_CONFIG.OWNER,
       P_F_UE_CONFIG.PARENT_OPERATION,
       P_F_UE_CONFIG.BUSY_OTHER,
       P_F_UE_CONFIG.SORT_TYPE,
       P_F_UE_CONFIG.LOCATION,
       P_F_UE_CONFIG.MOQ_SUBENTITY_LOOKUP,
       P_F_UE_CONFIG.OWNED_BY,
       P_F_UE_CONFIG.ALLOWED_VIRTUAL_LINE_ID,
       P_F_UE_CONFIG.ALLOWED_VIRTUAL_LINE,
       P_F_UE_CONFIG.PARENT_ENTITY,
       P_F_UE_CONFIG.ENTITY_TYPE,
       P_F_UE_CONFIG.AVAILABILITY_FACTOR,
       P_F_UE_CONFIG.LAST_UPDATE_DATE,
       P_F_UE_CONFIG.LAST_UPDATE_USER,
       P_F_UE_CONFIG.SECURITY_CODE
FROM D1D_PROD_XEUSS.P_F_UE_CONFIG P_F_UE_CONFIG
"""



for i, row in new_row.iterrows():
    date = new_row.loc[i, 'DATE']
    new_date = date[0:10]+ " " +date[11:19]
    new_row.loc[i,'DATE'] = new_date


conn_XEUS = PyUber.connect(datasource=('D1D_PROD_XEUS'))
atlas_frame = pd.read_sql(atlas_sql, conn_XEUS)
UE_frame = pd.read_sql(UE_sql,conn_XEUS)
atlas_output  = pd.DataFrame(columns= atlas_frame.columns)
UE_output = pd.DataFrame(columns= UE_frame.columns)
print(new_row)

for j, row in new_row.iterrows():
    if(new_row.loc[0, 'VALUE'] == 'UTP'):
        for i,row in atlas_frame.iterrows():
            if (atlas_frame.loc[i,'ENTITY'] == new_row.loc[j,'ENTITY']):
                atlas_frame.loc[i,'EXCLUDETODATE'] = datetime.now()
                atlas_output =  pd.concat([atlas_output,atlas_frame.iloc[[i]]])
                break
    elif(new_row.loc[0, 'VALUE'] == 'Decon'):
        for i,row in atlas_frame.iterrows():
            if (atlas_frame.loc[i,'ENTITY'] == new_row.loc[j,'ENTITY']):
                atlas_output =  pd.concat([atlas_output,atlas_frame.iloc[[i]]])
                atlas_output.loc[j,'EXCLUDETODATE'] = new_row.loc[j,'DATE']
                break  
    if(new_row.loc[0, 'VALUE'] == 'UTP'):
        for i,row in UE_frame.iterrows():
            if (UE_frame.loc[i,'ENTITY'] == new_row.loc[j,'ENTITY']):
                UE_frame.loc[i, 'INCLUDE'] = 'Y'
                UE_output = pd.concat([UE_output, UE_frame.iloc[[i]]])
                break
    elif(new_row.loc[0, 'VALUE'] == 'Decon'):
        for i,row in atlas_frame.iterrows():
            if (UE_frame.loc[i,'ENTITY'] == new_row.loc[j,'ENTITY']):
                UE_frame.loc[i, 'INCLUDE'] = 'N'
                UE_output = pd.concat([UE_output, UE_frame.iloc[[i]]])  
                break  

print(atlas_output)
print(UE_output)
atlas_output.to_csv(r"C:\Users\bhowell\Downloads\test.csv")


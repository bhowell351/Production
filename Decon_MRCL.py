 
import numpy as np
import time, PyUber, os, csv
import warnings
from datetime import datetime
import pandas as pd


ent_input = pd.read_csv(r"C:\Users\bhowell\OneDrive - Intel Corporation\ME\STRS upload test.csv")
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



for i, row in ent_input.iterrows():
    date = ent_input.loc[i, 'DATE']
    new_date = date[0:10]+ " " +date[11:19]
    ent_input.loc[i,'DATE'] = new_date


conn_XEUS = PyUber.connect(datasource=('D1D_PROD_XEUS'))
atlas_frame = pd.read_sql(atlas_sql, conn_XEUS)
UE_frame = pd.read_sql(UE_sql,conn_XEUS)
atlas_output  = pd.DataFrame(columns= atlas_frame.columns)
UE_output = pd.DataFrame(columns= UE_frame.columns)
dne_output = pd.DataFrame(columns = ['ENTITY'])


for j, row in ent_input.iterrows():
    not_exist = True
    if(ent_input.loc[j, 'VALUE'] == 'UTP'):
        for i,row in atlas_frame.iterrows():
            if (atlas_frame.loc[i,'ENTITY'] == ent_input.loc[j,'ENTITY']):
                not_exist = False
                atlas_output =  pd.concat([atlas_output,atlas_frame.iloc[[i]]])
                atlas_output.loc[i,'EXCLUDETODATE'] = datetime.now()
                break
    elif(ent_input.loc[j, 'VALUE'] == 'Decon'):
        for i,row in atlas_frame.iterrows():
            if (atlas_frame.loc[i,'ENTITY'] == ent_input.loc[j,'ENTITY']):
                not_exist = False
                atlas_output =  pd.concat([atlas_output,atlas_frame.iloc[[i]]])
                atlas_output.loc[i,'EXCLUDEFROMDATE'] = ent_input.loc[j,'DATE']
                atlas_output.loc[i,'EXCLUDETODATE']  = '2999-12-31 12:00:00'
                break  
    if(ent_input.loc[j, 'VALUE'] == 'UTP'):
        for i,row in UE_frame.iterrows():
            if (UE_frame.loc[i,'ENTITY'] == ent_input.loc[j,'ENTITY']):
                not_exist = False
                UE_frame.loc[i, 'INCLUDE'] = 'Y'
                UE_output = pd.concat([UE_output, UE_frame.iloc[[i]]])
                break
    elif(ent_input.loc[j, 'VALUE'] == 'Decon'):
        for i,row in atlas_frame.iterrows():
            if (UE_frame.loc[i,'ENTITY'] == ent_input.loc[j,'ENTITY']):
                not_exist = False
                UE_frame.loc[i, 'INCLUDE'] = 'N'
                UE_output = pd.concat([UE_output, UE_frame.iloc[[i]]])  
                break  
    if(not_exist == True):
        dne_output.loc[-1]  = ent_input.loc[j,'ENTITY']
        dne_output.index = dne_output.index + 1
        dne_output = dne_output.sort_index()
empty_csv = pd.DataFrame(columns = ent_input.columns)
atlas_output.to_csv(r"\\rf3p-nas-XEUS_Reports.rf3prod.mfg.intel.com\XEUS_Reports\AFO\P1274\General\AFO\STRS\F_UDF_ATLAS_EQP_EXCLUDE\Atlas_MRCL.csv", index = False)
UE_output.to_csv(r"\\rf3p-nas-XEUS_Reports.rf3prod.mfg.intel.com\XEUS_Reports\AFO\P1274\General\AFO\STRS\F_UE_CONFIG\UE_MRCL.csv", index = False)
dne_output.to_csv(r"C:\Users\bhowell\OneDrive - Intel Corporation\ME\Decon_MRCL\Tool_Exist.csv", index = False)
empty_csv.to_csv(r"C:\Users\bhowell\OneDrive - Intel Corporation\ME\STRS upload test.csv", index = False)

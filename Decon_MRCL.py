
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

atlas_columns = ['FACILITY','ENTITY','EXCLUDEFROMDATE','EXCLUDETODATE','LAST_UPDATE_DATE', 'LAST_UPDATE_USER','SECURITY_CODE','COMMENTS']
UE_columns = ['FACILITY', 'ENTITY', 'INCLUDE']
REV_columns = ['FACILITY', 'ENTITY']

for i, row in new_row.iterrows():
    date = new_row.loc[i, 'DATE']
    new_date = date[0:10]+ " " +date[11:19]
    new_row.loc[i,'DATE'] = new_date


conn_XEUS = PyUber.connect(datasource=('D1D_PROD_XEUS'))
atlas_frame = pd.read_sql(atlas_sql, conn_XEUS)
if(new_row.loc[0, 'VALUE'] == 'UTP'):
    for i,row in atlas_frame.iterrows():
        if (atlas_frame.loc[i,'ENTITY'] == new_row.loc[0,'ENTITY']):
            print(atlas_frame.loc[i,'ENTITY'] )
            break

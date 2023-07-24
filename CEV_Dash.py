
import numpy as np
import time, PyUber, os, csv
import warnings
from datetime import datetime
import pandas as pd

warnings.simplefilter("ignore")
conn_XEUS = PyUber.connect(datasource=('D1D_PROD_XEUS'))

query = """
SELECT P_F_AM_F3.AM_LDR_PATH,
       P_F_AM_F3.AM_LDR_PROCESS,
       P_F_AM_F3.AM_LDR_MODELNAME,
       P_F_AM_F3.AM_LDR_OBJECTNAME,
       P_F_AM_F3.ROW_ID,
       P_F_AM_F3.ROW_ORDER,
       P_F_AM_F3.ENTITY,
       SUBSTR(P_F_AM_F3.COMMENTS,2,5) AS PROCESS,
       P_F_AM_F3.OPERATION,
       P_F_AM_F3.COMMENTS,
       P_F_AM_F3.PARAMETER_LIST,
       P_F_AM_F3.LOAD_DATE,
       P_F_AM_F3.SECURITY_CODE
FROM D1D_PROD_XEUS.P_F_AM_F3 P_F_AM_F3
WHERE     (P_F_AM_F3.AM_LDR_PATH LIKE '%D1D.FBE.FBE Plating.CEV%')
      AND (P_F_AM_F3.AM_LDR_PROCESS = 'MFG')
      AND (P_F_AM_F3.AM_LDR_OBJECTNAME = 'F3_SETUP')
"""

dframe = pd.read_sql(query, conn_XEUS)
dframe.to_csv(r"C:\Users\bhowell\OneDrive - Intel Corporation\Documents\dframe.csv", index = False)
dframe['PARAMETER_LIST'] = dframe['PARAMETER_LIST'].str.split(';')
dfr = dframe.explode('PARAMETER_LIST')
chem = dfr.loc[dfr['PARAMETER_LIST'].str.contains("LAYER=", case = False)]
output = pd.DataFrame({'OPERATION':chem['OPERATION'],'PROCESS':chem['PROCESS'],'GROUPING':chem['PARAMETER_LIST']})
output.drop_duplicates(inplace = True)
for i, row in output.iterrows():
      output.loc[i,'GROUPING'] = output.loc[i,'GROUPING'][6:]
print(output)

output.to_csv(r"C:\Users\bhowell\OneDrive - Intel Corporation\Documents\CEV Dashboard\oper_to_layer.csv", index = False)

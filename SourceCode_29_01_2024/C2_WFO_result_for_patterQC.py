#%%
###Importing libraries
import os
import sys
import fnmatch
import pandas as pd
import numpy as np
import chardet
from dateutil.relativedelta import relativedelta
from scipy.spatial import distance_matrix
from scipy import interpolate
pd.set_option('display.max_columns', 1000)
import datetime
import ast
#%%% setting code working folder
code_folder = "//asia-pac.shell.com/tcs/bng/pt.simpl/proj/epx1/SAA-PDO/2021/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput
import WFO_Functions as fn

#%%% defining column headers as variables

ab_name="Abbr Name"
field_code="FIELD_CODE"
composite_id="COMPOSITE_ID"
w_type="WELL_TYPE"

ab_name1="Abbr_Name"
dat="DATES"
uniqueid="UNIQUEID"
wellbore="WELLBORE"
wb_sn= "WB_SHORTNAME"
w_type="C_WELL_TYPE"
pt_nm="PAT_NAME"
compl_status="COMPL_STATUS"
compl_start="COMPL_START"






#%%Importing WFO result file and taking out list of all the connected injectors to a producer


df_wfo_result=pd.read_csv(userinput.output_folder+"/"+userinput.wfo_result_output)
df_wfo_result1 = df_wfo_result[['s_WB_SHORTNAME', 's_COMPOSITE_ID', 's_C_WELL_TYPE', 's_COMPL_STATUS','COMPOSITE_ID',
                                'WB_SHORTNAME', 'C_WELL_TYPE', 'COMPL_STATUS','RES_CODE','FIELD_CODE','FIELD_RES_GROUP','ACTIVE_PAIR_FLAG','CF_STATIC_P','CF_SCORE_DYNA_P', 'CF_DYNA_P',"PAT_NAME"]]

df_pattern=pd.read_csv(userinput.output_folder+"/"+userinput.pattern_map_output)

dict_all_inj={}
dict_all_inj_mag={}
for pdd, wfpd in df_wfo_result1.groupby([composite_id]):
    wfpd1 = wfpd[wfpd['ACTIVE_PAIR_FLAG']>0]
    wi_all=[]
    wi_open=[]
    wi_all_mag=[]
    for i, r in wfpd1.iterrows():
        if r["s_"+w_type]=="WI":
            a =r["s_"+composite_id].split("_")[1][-1]
            if a=="S" or a=="L":
                main_inj=r["s_"+wb_sn]+r["s_"+composite_id].split("_")[1][-1]
            else:
                main_inj=r["s_"+wb_sn]
            # print(pdd, a,main_inj)
            [wi_all.append(x) for x in [r["s_"+wb_sn]] if x not in wi_all]
            [wi_all_mag.append(x) for x in [r['CF_DYNA_P']]]
            print(pdd,wi_all)
        else:
            continue
        dict_all_inj.update({pdd:wi_all})
df_wfo_result1["CONN_INJ_OPEN"]=df_wfo_result1[composite_id].map(dict_all_inj)


df_wfo_result1["PAT_MAP_CONN_INJ"]=df_wfo_result1[wb_sn].map(dict(zip(df_pattern[wb_sn],df_pattern["CONN_OPEN_INJ"])))
#%%
df_wfo_result_temp1=df_wfo_result1[~df_wfo_result1["PAT_MAP_CONN_INJ"].isnull()]
df_wfo_result_temp2=df_wfo_result1[df_wfo_result1["PAT_MAP_CONN_INJ"].isnull()]
df_wfo_result_temp1["PAT_MAP_CONN_INJ"]=df_wfo_result_temp1["PAT_MAP_CONN_INJ"].apply(lambda x: ast.literal_eval(x) if x!=np.nan else np.nan)
df_wfo_result1=pd.concat([df_wfo_result_temp1,df_wfo_result_temp2])

df_wfo_result1["PAT_MATCH_SCORE"]=df_wfo_result1.apply(lambda x:fn.pat_match_score(x["CONN_INJ_OPEN"],x["PAT_MAP_CONN_INJ"],-1),axis=1)


#%%Exporting pattern mapping file


df_wfo_result1.to_csv(userinput.output_folder+"/"+"P2_WFO_Result_forpatternmap.csv", index=False)
print("*********************************************************************************")
print("Exported :"+"P2_WFO_Result_forpatternmap.csv")
print("*********************************************************************************")

# %%

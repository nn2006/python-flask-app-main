# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:27:06 2021

@author: Prateeksha.M
@author: R.Agrawal

1-Dec-2021:r.agrawal
added code part for calculating latest 3 month avg production and injection
data and combining with W_Master.

"""



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
from sympy import composite
pd.set_option('display.max_columns', 1000)
import datetime
import warnings
warnings.filterwarnings("ignore")
#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput


#%%% setting code working folder
manual_data_folder =userinput.manualdata_input_folder
static_data_folder = userinput.staticdata_input_folder
dynamic_data_folder = userinput.dynamicdata_input_folder
output_folder=userinput.output_folder
dbtables_folder = userinput.dbtables_folder1 



#%%% defining column headers as variables

ab_name="Abbr Name"
field_code="FIELD_CODE"
composite_id="COMPOSITE_ID"
w_type="C_WELL_TYPE"
compl_mid_x="COMPL_MID_X"
compl_mid_y="COMPL_MID_Y"
compl_start="COMPL_START"
compl_status="COMPL_STATUS"
inv_dist="DISTANCE_INV"
v_dist="DISTANCE"
res_open="RES_CODE_OPEN"
res_close="RES_CODE_CLOSE"
zn_close="ZONE_CLOSED"
zn_open="ZONE_OPEN"
compl_mid_md="COMPL_MID_MD"
ab_name1="Abbr_Name"
dat="DATES"
uniqueid="UNIQUEID"
wellbore="WELLBORE"
holename="HOLE_NAME"
datt1="MONTH_START_DATE"
wbname= "WELLBORE_NAME"

#%%Reading respective tables from database
def load_dataframe(dbfolder,filename):
    with open(os.path.join(dbfolder,filename), 'rb') as f:
        result = chardet.detect(f.read()) 
    df= pd.read_csv(os.path.join(dbfolder,filename),encoding=result['encoding'])
    return(df)

df_v_well=load_dataframe(userinput.dbtables_folder1,userinput.v_well) 
df_v_well_comp=load_dataframe(userinput.dbtables_folder1,userinput.v_well_comp_int)
df_v_wb_dev=load_dataframe(userinput.dbtables_folder1,userinput.v_wb_dev)
df_v_mon_prod=load_dataframe(userinput.dbtables_folder1,userinput.v_mon_prod)
df_v_mon_prod[datt1]=pd.to_datetime(df_v_mon_prod[datt1])
df_v_mon_prod["MONTH_END_DATE"]=pd.to_datetime(df_v_mon_prod["MONTH_END_DATE"])

df_mas= pd.merge(df_v_well,df_v_well_comp, on="WELL_NAME",how="outer")
df_mas['COMPLETION_START_DATE']=pd.to_datetime(df_mas['COMPLETION_START_DATE'])
df_mas['SPUD_DATE']=pd.to_datetime(df_mas['SPUD_DATE'])

#%%The section is based on old files
# df_masterxy=load_dataframe(dbtables_folder,userinput.master_xy)
# df_masterxy["CMPLN_INT_DATE"]=pd.to_datetime(df_masterxy["CMPLN_INT_DATE"])
# # df_masterxy["CMPLN_INT_DATE"]=df_masterxy["CMPLN_INT_DATE"].apply(lambda x: datetime.date(year=x.date().year,day=x.date().month,month=x.date().day) if pd.notnull(x) else '')
# # df_masterxy["CMPLN_INT_DATE"]=pd.to_datetime(df_masterxy["CMPLN_INT_DATE"])

# df_deviation=load_dataframe(userinput.deviation)

# df_prod=load_dataframe(userinput.monthly_prod)
# df_prod[dat]=pd.to_datetime(df_prod[dat])
# # df_prod[dat]=df_prod[dat].apply(lambda x: datetime.date(year=x.date().year,day=x.date().month,month=x.date().day))
# # df_prod[dat]=pd.to_datetime(df_prod[dat])

# df_wi=load_dataframe(userinput.monthly_wi)
# df_wi[dat]=pd.to_datetime(df_wi[dat])
# # df_wi[dat]=df_wi[dat].apply(lambda x: datetime.date(year=x.date().year,day=x.date().month,month=x.date().day))
# # df_wi[dat]=pd.to_datetime(df_wi[dat])
#%% Manually mainted reservior grouping
df_resmap = pd.read_excel(os.path.join(manual_data_folder,userinput.rescode_mapping_file))


#%%%Calculating PD for production and injection data

days_in_prod="NO_OF_DAYS_PRODUCTION"


df_v_mon_prod["C_LIQUID"]=df_v_mon_prod["MN_PROD_OIL"]+df_v_mon_prod["MN_PROD_WATER"]
df_v_mon_prod["C_WCT"]=df_v_mon_prod["MN_PROD_WATER"]/df_v_mon_prod["C_LIQUID"]

df_v_mon_prod["C_OIL_PD"]=df_v_mon_prod["MN_PROD_OIL"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_OIL_PD"]=df_v_mon_prod["C_OIL_PD"].replace(np.inf, 0)
df_v_mon_prod["C_GAS_PD"]=df_v_mon_prod["MN_PROD_GAS"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_GAS_PD"]=df_v_mon_prod["C_GAS_PD"].replace(np.inf, 0)
df_v_mon_prod["C_WATER_PD"]=df_v_mon_prod["MN_PROD_WATER"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_WATER_PD"]=df_v_mon_prod["C_WATER_PD"].replace(np.inf, 0)
df_v_mon_prod["C_LIFT_GAS_PD"]=df_v_mon_prod["VOL_LIFT_GAS"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_LIFT_GAS_PD"]=df_v_mon_prod["C_LIFT_GAS_PD"].replace(np.inf, 0)
df_v_mon_prod["C_GASINJ_PD"]=df_v_mon_prod["MN_GAS_INJ"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_GASINJ_PD"]=df_v_mon_prod["C_GASINJ_PD"].replace(np.inf, 0)

df_v_mon_prod["C_INJ_PD"]=df_v_mon_prod["MN_WATER_INJ"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_INJ_PD"]=df_v_mon_prod["C_INJ_PD"].replace(np.inf, 0)
df_v_mon_prod["C_STEAM_INJ_PD"]=df_v_mon_prod["MN_STEAM_INJ"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_STEAM_INJ_PD"]=df_v_mon_prod["C_STEAM_INJ_PD"].replace(np.inf, 0)

df_v_mon_prod["C_LIQ_PD"]=df_v_mon_prod["C_LIQUID"]/df_v_mon_prod[days_in_prod]
df_v_mon_prod["C_LIQ_PD"]=df_v_mon_prod["C_LIQ_PD"].replace(np.inf, 0)

df_v_mon_prod["C_WCT_PD"]=df_v_mon_prod["C_WATER_PD"]/df_v_mon_prod["C_LIQ_PD"]
df_v_mon_prod["C_WCT_PD"]=df_v_mon_prod["C_WCT_PD"].replace(np.inf, 0)

#%%PD calculations based on old data inputs

# df_prod["C_LIQUID"]=df_prod["OIL"]+df_prod["WATER"]
# df_prod["C_WCT"]=df_prod["WATER"]/df_prod["C_LIQUID"]

# df_prod["C_DAYS_MONTH"]= df_prod[dat].dt.days_in_month
# df_prod["C_OIL_PD"]=df_prod["OIL"]/df_prod["DAYS"]
# df_prod["C_OIL_PD"]=df_prod["C_OIL_PD"].replace(np.inf, 0)
# df_prod["C_GAS_PD"]=df_prod["GAS"]/df_prod["DAYS"]
# df_prod["C_GAS_PD"]=df_prod["C_GAS_PD"].replace(np.inf, 0)
# df_prod["C_WATER_PD"]=df_prod["WATER"]/df_prod["DAYS"]
# df_prod["C_WATER_PD"]=df_prod["C_WATER_PD"].replace(np.inf, 0)
# df_prod["C_LIFT_GAS_PD"]=df_prod["LIFT_GAS"]/df_prod["DAYS"]
# df_prod["C_LIFT_GAS_PD"]=df_prod["C_LIFT_GAS_PD"].replace(np.inf, 0)
# df_prod["C_GASINJ_PD"]=df_prod["GASINJ"]/df_prod["DAYS"]
# df_prod["C_GASINJ_PD"]=df_prod["C_GASINJ_PD"].replace(np.inf, 0)

# df_prod["C_LIQ_PD"]=df_prod["C_LIQUID"]/df_prod["DAYS"]
# df_prod["C_LIQ_PD"]=df_prod["C_LIQ_PD"].replace(np.inf, 0)

# df_prod["C_WCT_PD"]=df_prod["C_WATER_PD"]/df_prod["C_LIQ_PD"]
# df_prod["C_WCT_PD"]=df_prod["C_WCT_PD"].replace(np.inf, 0)

#%%%renaming columns to get _CD rates columns
df_v_mon_prod=df_v_mon_prod.rename(columns ={"MN_PROD_OIL_CD":"C_OIL_CD","MN_PROD_WATER_CD":"C_WATER_CD",
                                "MN_GAS_INJ_CD":"C_GASINJ_CD","MN_PROD_GAS_CD":"C_GAS_CD",
                                "MN_WATER_INJ_CD":"C_INJ_CD","VOL_LIFT_GAS_CD":"C_LIFT_GAS_CD"})

df_v_mon_prod["C_LIQ_CD"]=df_v_mon_prod["C_OIL_CD"]+df_v_mon_prod["C_WATER_CD"]
df_v_mon_prod["C_WCT_CD"]=df_v_mon_prod["C_WATER_CD"]/df_v_mon_prod["C_LIQ_CD"]

df_v_mon_prod[uniqueid]=df_v_mon_prod["COMPLETION"]

df_v_mon_prod.sort_values(by=[uniqueid],inplace=True)
df_v_mon_prod.sort_values(by=[datt1],inplace=True)
df_v_mon_prod["C_CUMOIL"]=df_v_mon_prod.groupby(uniqueid)["MN_PROD_OIL"].cumsum()
df_v_mon_prod["C_CUMWATER"]=df_v_mon_prod.groupby(uniqueid)["MN_PROD_WATER"].cumsum()
df_v_mon_prod["C_CUMGAS"]=df_v_mon_prod.groupby(uniqueid)["MN_PROD_GAS"].cumsum()
df_v_mon_prod["C_CUMGASINJ"]=df_v_mon_prod.groupby(uniqueid)["MN_GAS_INJ"].cumsum()

df_v_mon_prod["C_CUMSTEAMINJ"]=df_v_mon_prod.groupby(uniqueid)["MN_STEAM_INJ"].cumsum()
df_v_mon_prod["C_CUMINJ"]=df_v_mon_prod.groupby(uniqueid)["MN_WATER_INJ"].cumsum()
df_v_mon_prod["C_CUMLIFTGAS"]=df_v_mon_prod.groupby(uniqueid)["VOL_LIFT_GAS"].cumsum()

df_v_mon_prod["C_CUMLIQ"]=df_v_mon_prod["C_CUMOIL"]+df_v_mon_prod["C_CUMWATER"]

df_v_mon_prod["LIQ_PLUS_INJ"]=df_v_mon_prod["C_LIQUID"]+df_v_mon_prod["MN_WATER_INJ"]

#%% Old injection file
# df_wi["C_DAYS_MONTH"]=df_wi[dat].dt.days_in_month
# df_wi["C_INJ_PD"]=df_wi["VOLUME"]/df_wi["DAYS"]
# df_wi["C_INJ_PD"]=df_wi["C_INJ_PD"].replace(np.inf, 0)
# df_wi["C_INJ_CD"]=df_wi["VOLUME"]/df_wi["C_DAYS_MONTH"]
# df_wi.sort_values(by=[uniqueid],inplace=True)
# df_wi.sort_values(by=[dat],inplace=True)
# df_wi["C_CUMINJ"]=df_wi.groupby(uniqueid)["VOLUME"].cumsum()
# df_wi.rename(columns={'DAYS':"DAYS_WI", 'VOLUME':"INJ"},inplace=True)
# df_wi.drop(columns=["C_DAYS_MONTH"],inplace=True)
# df_prod_inj = pd.merge(df_prod,df_wi,on=[uniqueid, 'DATES'],how="outer")
# df_prod_inj["LIQ_PLUS_INJ"]=df_prod_inj["C_LIQUID"]+df_prod_inj["INJ"]

#%%# %% Define well status open, close, well type and downhole completion type

df_prod_inj = df_v_mon_prod.copy()

df_status = pd.DataFrame()
for uid, dfuid in df_prod_inj.groupby([uniqueid]): 
    dftemp=pd.DataFrame(index=None)
    dftemp[uniqueid]=[uid]
    dftemp["C_MAX_CUMOIL"] =[dfuid["C_CUMOIL"].max()]
    dftemp["C_MAX_CUMWATER"] =[dfuid["C_CUMWATER"].max()]
    dftemp["C_MAX_CUMINJ"] =[dfuid["C_CUMINJ"].max()]
    dftemp["C_MAX_CUMLIQ"]=[dfuid["C_CUMLIQ"].max()]
    if dfuid["C_CUMINJ"].max()==0:
        if dfuid["C_CUMOIL"].max()>0:
            dftemp[w_type]=["OP"]
        else:
            dftemp[w_type]=np.nan
    else:
        if dfuid["C_CUMLIQ"].max()==0:
            dftemp[w_type]=["WI"]
        else:
            dftemp[w_type]=["OP-WI"]
    dfuid1 =dfuid[dfuid["LIQ_PLUS_INJ"]>10].reset_index(drop=True)
    if len(dfuid1)>0:
        dfuid1.sort_values([datt1], inplace=True)    
        dftemp["COMPL_START"] = dfuid1[datt1].min()
        dftemp["COMPL_END"] = dfuid1[datt1].max()
        if dfuid1[datt1].max() > (df_prod_inj[datt1].max() - relativedelta(days= userinput.active_well_window_days)):
            dftemp[compl_status] ="OPEN"
        else:
            dftemp[compl_status] = "CLOSED"
        dftemp["C_OIL_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_OIL_PD'])
        dftemp["C_WATER_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_WATER_PD'])
        dftemp["C_GAS_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_GAS_PD'])
        dftemp["C_LIFT_GAS_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_LIFT_GAS_PD'])
        dftemp["C_GASINJ__PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_GASINJ_PD'])
        dftemp["C_LIQ_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_LIQ_PD'])
        dftemp["C_INJ_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_INJ_PD'])
        dftemp["C_WCT_PD_LAST"]=pd.Series(dfuid1.loc[dfuid1[datt1].idxmax(),'C_WCT_PD'])
        dftemp['C_Total_Years_Prd_Inj'] = (dfuid1[datt1].max()-dfuid1[datt1].min())/np.timedelta64(1,'Y')
    else:
        dftemp["COMPL_START"] = np.nan
        dftemp["COMPL_END"] = np.nan
        dftemp[compl_status] = "CLOSED"
        dftemp["C_OIL_PD_LAST"]=np.nan
        dftemp["C_WATER_PD_LAST"]=np.nan
        dftemp["C_GAS_PD_LAST"]=np.nan
        dftemp["C_LIFT_GAS_PD_LAST"]=np.nan
        dftemp["C_GASINJ__PD_LAST"]=np.nan
        dftemp["C_LIQ_PD_LAST"]=np.nan
        dftemp["C_INJ_PD_LAST"]=np.nan
        dftemp["C_WCT_PD_LAST"]=np.nan
        dftemp['C_Total_Years_Prd_Inj'] = np.nan
    df_status=pd.concat([df_status,dftemp])

#%%Getting 1 year avg rates
ref_date=df_prod_inj[datt1].max()
req_prod_temp = df_prod_inj[df_prod_inj[datt1]>=ref_date-relativedelta(months=12)]
req_prod_temp1=req_prod_temp.groupby(uniqueid).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
req_prod_temp1.columns = [x+"_1yr_avg" for x in req_prod_temp1.columns]
req_prod_temp1.reset_index(inplace=True)

req_prod_temp = df_prod_inj[df_prod_inj[datt1]>=ref_date-relativedelta(months=6)]
req_prod_temp2=req_prod_temp.groupby(uniqueid).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
req_prod_temp2.columns = [x+"_6mon_avg" for x in req_prod_temp2.columns]
req_prod_temp2.reset_index(inplace=True)

req_prod_temp = df_prod_inj[df_prod_inj[datt1]>=ref_date-relativedelta(months=3)]
req_prod_temp3=req_prod_temp.groupby(uniqueid).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
req_prod_temp3.columns = [x+"_3mon_avg" for x in req_prod_temp3.columns]
req_prod_temp3.reset_index(inplace=True)

req_prod_temp = df_prod_inj[df_prod_inj[datt1]>=ref_date-relativedelta(months=1)]
req_prod_temp4=req_prod_temp.groupby(uniqueid).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
req_prod_temp4.columns = [x+"_1mon_avg" for x in req_prod_temp4.columns]
req_prod_temp4.reset_index(inplace=True)

req_prod_temp = df_prod_inj[df_prod_inj[datt1]>=ref_date-relativedelta(days=7)]
req_prod_temp5=req_prod_temp.groupby(uniqueid).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
req_prod_temp5.columns = [x+"_1week_avg" for x in req_prod_temp5.columns]
req_prod_temp5.reset_index(inplace=True)

req_prod_temp = df_prod_inj[df_prod_inj[datt1]>=ref_date-relativedelta(days=1)]
req_prod_temp6=req_prod_temp.groupby(uniqueid).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
req_prod_temp6.columns = [x+"_last_avg" for x in req_prod_temp6.columns]
req_prod_temp6.reset_index(inplace=True)




#%%Getting completion mid x and y for each well based on devaition data and surface cooridnates        

df_deviation = df_v_wb_dev.copy()
df_deviation =df_deviation.rename(columns={"DEFLECTION_EW":"XDELT","DEFLECTION_NS":"YDELT"})
welluwi="WELL_UWI"

df_deviation["XCOORD"]= df_deviation[welluwi].map(dict(zip(df_mas[welluwi], df_mas["EASTING"])))
df_deviation["YCOORD"]=df_deviation[welluwi].map(dict(zip(df_mas[welluwi], df_mas["NORTHING"])))
df_deviation['PROD_INTV_MID']=df_deviation[welluwi].map(dict(zip(df_mas[welluwi], df_mas['MID_PROD_INTERVAL_DFE'])))

df_deviation["X"]=df_deviation["XCOORD"]+df_deviation["XDELT"]
df_deviation["Y"]=df_deviation["YCOORD"]+df_deviation["YDELT"]

dfmid=pd.DataFrame()
for wbore, dfdev in df_deviation.groupby(holename):
    dfmidtemp=pd.DataFrame(index=None)
    dfmidtemp[holename]=[wbore]
    fx = interpolate.interp1d(dfdev["AHD_DFE"], dfdev["X"]) #defining relation between "MD" and "X" as function
    fy = interpolate.interp1d(dfdev["AHD_DFE"], dfdev["Y"]) #defining relation between "MD" and "Y" as function
    # dfmidtemp[compl_mid_x] = fx(dfdev["PROD_INTV_MID"].max()) 
    # dfmidtemp[compl_mid_y] = fy(dfdev["PROD_INTV_MID"].max())
    try: 
        dfmidtemp[compl_mid_x] = fx(dfdev["PROD_INTV_MID"].max()) 
        dfmidtemp[compl_mid_y] = fy(dfdev["PROD_INTV_MID"].max())
        # print(wbore)
    except:
        dfmidtemp[compl_mid_x] = np.nan
        dfmidtemp[compl_mid_y] = np.nan
        # print('Exception '+wbore)
        continue
    dfmid=pd.concat([dfmid,dfmidtemp])

df_mas[compl_mid_x]=df_mas[wbname].map(dict(zip(dfmid[holename],dfmid[compl_mid_x])))
df_mas[compl_mid_y]=df_mas[wbname].map(dict(zip(dfmid[holename],dfmid[compl_mid_y])))
df_mas["DEV_DATA_AVBL"] = df_mas.apply(lambda x: "Y" if (x[wbname] in dfmid[holename].unique()) ==True else "N" , axis=1)

df_mas.loc[df_mas[compl_mid_x].isna(), compl_mid_x] = df_mas.loc[df_mas[compl_mid_x].isna(), "EASTING"]
df_mas.loc[df_mas[compl_mid_y].isna(), compl_mid_y] = df_mas.loc[df_mas[compl_mid_y].isna(), "NORTHING"]

##making uniqueid column in new well master table
# df_mas["UNIQUEID"]= df_mas[wbname]+":"+df_mas["COMPLETION_INTERVAL_ID"]+":"+df_mas["RESERVOIR_CODE"]
df_mas["UNIQUEID"]= df_mas[wbname]+":"+df_mas["COMPLETION"]
df_mas = pd.merge(df_mas,df_status, on =uniqueid, how='left')
df_mas = pd.merge(df_mas,req_prod_temp1, on =uniqueid, how='left')
df_mas = pd.merge(df_mas,req_prod_temp2, on =uniqueid, how='left')
df_mas = pd.merge(df_mas,req_prod_temp3, on =uniqueid, how='left')
df_mas = pd.merge(df_mas,req_prod_temp4, on =uniqueid, how='left')
df_mas = pd.merge(df_mas,req_prod_temp5, on =uniqueid, how='left')
df_mas = pd.merge(df_mas,req_prod_temp6, on =uniqueid, how='left')

df_mas["ANA_START_DATE"]=ref_date-relativedelta(months=12)
df_mas["ANA_END_DATE"]=ref_date


#%%
#Add dual string, single string tags for active wellbores-----------------------------------------
wb_df1 =pd.DataFrame()
for wb, wdf in df_mas.groupby(wbname):
    wdf = wdf[wdf[compl_status] =='OPEN']
    if len(wdf)>0:
        wdf["CND_COUNT"] = wdf['CONDUIT_NAME'].nunique()
        wdf["WB_COMP_TYPE"] = wdf.apply(lambda x: "DUAL_STRING" if x["CND_COUNT"]>1 else "SINGLE_STRING", axis=1)
        wb_df1 = wb_df1.append(wdf)
df_mas["WB_COMP_TYPE"]= df_mas[wbname].map(dict(zip(wb_df1[wbname], wb_df1["WB_COMP_TYPE"])))  
df_mas["FIELD_RES_temp"] = df_mas["FIELD_NAME"].str.upper()+"_"+df_mas["RESERVOIR_NAME"]
df_resmap["FIELD_RES_temp"]=df_resmap["FIELD_NAME"]+"_"+df_resmap["RESERVOIR"]
df_mas["c_RESERVOIR"] = df_mas["FIELD_RES_temp"].map(dict(zip(df_resmap["FIELD_RES_temp"], df_resmap["c_RESERVOIR"])))
df_mas=df_mas[~df_mas[wbname].isnull()]

#Add commingled/ dedicated flags for each wellbore conduit----------------------------------------   
df_mas[wbname+"_CND"] = df_mas.apply(lambda x: x[wbname]+"_"+str(x["CONDUIT_NAME"]), axis=1)  
wc_df1 = pd.DataFrame()  
for wb_cnd, cdf in df_mas.groupby(wbname+"_CND"):
    cdf = cdf[cdf[compl_status]=='OPEN']
    if len(cdf)>0:
        cdf["RES_COUNT"] = cdf['c_RESERVOIR'].nunique()
        cdf["RES_COMP_TYPE"] = cdf.apply(lambda x: "COMMINGLED" if x["RES_COUNT"]>1 else "DEDICATED", axis=1)
        
        o_zone = cdf["c_RESERVOIR"].tolist()    
        o_zone = list(dict.fromkeys(o_zone))
        cdf[zn_open] = '[%s]' % ', '.join(map(str, o_zone))
        wc_df1 = wc_df1.append(cdf)
df_mas["RES_COMP_TYPE"]= df_mas[wbname+"_CND"].map(dict(zip(wc_df1[wbname+"_CND"], wc_df1["RES_COMP_TYPE"])))  
df_mas[zn_open]= df_mas[wbname+"_CND"].map(dict(zip(wc_df1[wbname+"_CND"], wc_df1[zn_open])))  
df_mas.rename(columns={wbname+"_CND":composite_id},inplace=True)
df_mas["RES_CODE"] = df_mas["FIELD_RES_temp"].map(dict(zip(df_resmap["FIELD_RES_temp"], df_resmap["RES_CODE"])))
# df_masterxy["RESERVOIR"] = df_masterxy["RESERVOIR"].map(dict(zip(df_resmap["RESERVOIR"], df_resmap["RES_CODE"])))
#%%
df_resmap["FIELD_RES_GROUP"]=df_resmap.apply(lambda x: str(x["FIELD_NAME"])+"_"+str(x["RES_CODE"]), axis=1) 
df_mas[field_code] = df_mas["FIELD_NAME"].str.upper()
df_mas["FIELD_RES_GROUP"] = df_mas.apply(lambda x: str(x[field_code])+"_"+str(x["RES_CODE"]), axis=1) 
df_mas["CRM_FLAG"]=df_mas["FIELD_RES_GROUP"].map(dict(zip(df_resmap["FIELD_RES_GROUP"], df_resmap["CRM_FLAG"])))
df_mas["STIR_FLAG"]=df_mas["FIELD_RES_GROUP"].map(dict(zip(df_resmap["FIELD_RES_GROUP"], df_resmap["STIR_FLAG"])))
df_mas["CRM_FLAG"].fillna(0,inplace=True)
df_mas["STIR_FLAG"].fillna(0,inplace=True)
df_mas.drop(columns=["FIELD_RES_temp"],inplace=True)

df_prod_inj[composite_id]=df_prod_inj[uniqueid].map(dict(zip(df_mas[uniqueid],df_mas[composite_id])))
df_prod_inj[w_type]=df_prod_inj[uniqueid].map(dict(zip(df_mas[uniqueid],df_mas[w_type])))
df_prod_inj["FIELD_NAME"]=df_prod_inj[uniqueid].map(dict(zip(df_mas[uniqueid],df_mas["FIELD_NAME"].str.upper())))
df_prod_inj["RES_CODE"]=df_prod_inj[uniqueid].map(dict(zip(df_mas[uniqueid],df_mas["RES_CODE"])))
df_prod_inj["RESERVOIR"]=df_prod_inj[uniqueid].map(dict(zip(df_mas[uniqueid],df_mas["RESERVOIR_NAME"])))
df_prod_inj["FIELD_RES_GROUP"] = df_prod_inj.apply(lambda x: str(x["FIELD_NAME"])+"_"+str(x["RES_CODE"]), axis=1) 
df_prod_inj["CRM_FLAG"]=df_prod_inj["FIELD_RES_GROUP"].map(dict(zip(df_resmap["FIELD_RES_GROUP"], df_resmap["CRM_FLAG"])))
df_prod_inj["STIR_FLAG"]=df_prod_inj["FIELD_RES_GROUP"].map(dict(zip(df_resmap["FIELD_RES_GROUP"], df_resmap["STIR_FLAG"])))
df_prod_inj["CRM_FLAG"].fillna(0,inplace=True)
df_prod_inj["STIR_FLAG"].fillna(0,inplace=True)
df_prod_inj[compl_status]=df_prod_inj[uniqueid].map(dict(zip(df_mas[uniqueid],df_mas[compl_status])))


#%% creating prod injection data file aggregated at composite_id considering only open conduits

df_prod_inj_uniqueid=df_prod_inj
df_prod_inj_compid=pd.DataFrame()
# df_prod_inj_uniqueid=df_prod_inj_uniqueid[df_prod_inj_uniqueid[compl_status]=="OPEN"]
df_prod_inj_uniqueid=df_prod_inj_uniqueid[df_prod_inj_uniqueid["STIR_FLAG"]==1]
   
df_temp1 = df_prod_inj_uniqueid.groupby([composite_id,datt1]).agg({'NO_OF_DAYS_PRODUCTION':"mean", 'MN_PROD_GAS':"sum", 'MN_PROD_WATER':"sum", 'MN_PROD_OIL':"sum", 'VOL_LIFT_GAS':"sum",
       'MN_GAS_INJ':"sum", 'C_LIQUID':"sum", 'C_WCT':"mean", 'C_OIL_PD':"sum", 'C_GAS_PD':"sum",
       'C_WATER_PD':"sum", 'C_LIFT_GAS_PD':"sum", 'C_GASINJ_PD':"sum", 'C_LIQ_PD':"sum", 'C_WCT_PD':"mean",
       'C_OIL_CD':"sum", 'C_GAS_CD':"sum", 'C_WATER_CD':"sum", 'C_LIFT_GAS_CD':"sum", 'C_GASINJ_CD':"sum",
       'C_LIQ_CD':"sum", 'C_WCT_CD':"mean", 'C_CUMOIL':"sum", 'C_CUMWATER':"sum", 'C_CUMGAS':"sum",
       'C_CUMGASINJ':"sum", 'C_CUMLIQ':"sum",  'MN_WATER_INJ':"sum", 'C_INJ_PD':"sum", 'C_INJ_CD':"sum",
       'C_CUMINJ':"sum", 'LIQ_PLUS_INJ':"sum", 'C_WELL_TYPE':"first", 'FIELD_NAME':"first",
       'RES_CODE':"first", 'FIELD_RES_GROUP':"first", 'CRM_FLAG':"mean", 'STIR_FLAG':"mean"})        
df_temp1=df_temp1.reset_index()
df_prod_inj_compid=df_temp1

# ref_date1=df_temp1[dat].max()
# req_prod_temp = df_temp1[df_temp1[dat]>=ref_date1-relativedelta(months=12)]
# req_prod_temp1=req_prod_temp.groupby(composite_id).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
# req_prod_temp1.columns = [x+"_1yr_avg" for x in req_prod_temp1.columns]
# req_prod_temp1.reset_index(inplace=True)

# req_prod_temp = df_temp1[df_temp1[dat]>=ref_date-relativedelta(months=6)]
# req_prod_temp2=req_prod_temp.groupby(composite_id).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
# req_prod_temp2.columns = [x+"_6mon_avg" for x in req_prod_temp2.columns]
# req_prod_temp2.reset_index(inplace=True)

# req_prod_temp = df_temp1[df_temp1[dat]>=ref_date-relativedelta(months=3)]
# req_prod_temp3=req_prod_temp.groupby(composite_id).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
# req_prod_temp3.columns = [x+"_3mon_avg" for x in req_prod_temp3.columns]
# req_prod_temp3.reset_index(inplace=True)

# req_prod_temp = df_temp1[df_temp1[dat]>=ref_date-relativedelta(months=1)]
# req_prod_temp4=req_prod_temp.groupby(composite_id).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
# req_prod_temp4.columns = [x+"_1mon_avg" for x in req_prod_temp4.columns]
# req_prod_temp4.reset_index(inplace=True)

# req_prod_temp = df_temp1[df_temp1[dat]>=ref_date-relativedelta(days=7)]
# req_prod_temp5=req_prod_temp.groupby(composite_id).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
# req_prod_temp5.columns = [x+"_1week_avg" for x in req_prod_temp5.columns]
# req_prod_temp5.reset_index(inplace=True)

# req_prod_temp = df_temp1[df_temp1[dat]>=ref_date-relativedelta(days=1)]
# req_prod_temp6=req_prod_temp.groupby(composite_id).agg({"C_OIL_PD":"mean","C_OIL_CD":"mean","C_WATER_PD":"mean","C_WATER_CD":"mean","C_LIQ_PD":"mean","C_LIQ_CD":"mean","C_INJ_PD":"mean","C_INJ_CD":"mean"})
# req_prod_temp6.columns = [x+"_last_avg" for x in req_prod_temp6.columns]
# req_prod_temp6.reset_index(inplace=True)


# df_masterxy = pd.merge(df_masterxy,req_prod_temp1, on =composite_id, how='left')
# df_masterxy = pd.merge(df_masterxy,req_prod_temp2, on =composite_id, how='left')
# df_masterxy = pd.merge(df_masterxy,req_prod_temp3, on =composite_id, how='left')
# df_masterxy = pd.merge(df_masterxy,req_prod_temp4, on =composite_id, how='left')
# df_masterxy = pd.merge(df_masterxy,req_prod_temp5, on =composite_id, how='left')
# df_masterxy = pd.merge(df_masterxy,req_prod_temp6, on =composite_id, how='left')

# end_date = min(df_prod["DATES"].max(),df_wi["DATES"].max())
# df_prod_inj_compid=df_prod_inj_compid[df_prod_inj_compid["DATES"]<=end_date]
# df_prod_inj=df_prod_inj[df_prod_inj["DATES"]<=end_date]
# %% export the processed production and masterxy file

df_mas.to_csv(output_folder+"/"+userinput.wmaster_output, index=False)
df_prod_inj.to_csv(output_folder+"/"+userinput.prod_inj_output1, index=False)
df_prod_inj_compid.to_csv(output_folder+"/"+userinput.prod_inj_output, index=False)


print("*********************************************************************************")
print("Exported :"+ userinput.wmaster_output)
print("Exported :"+ userinput.prod_inj_output1)
print("Exported :"+ userinput.prod_inj_output)
print("*********************************************************************************")


# %%

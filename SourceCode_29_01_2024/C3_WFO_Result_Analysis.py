# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 11:51:04 2022

@author: R.Agrawal

File Description: This file defines the calculation for aggregating rates at pattern level taking into account estimated connectivity factors

    
Input description:
1) P2_WFO_RESULT.csv goes as input    
2) Pattern Attributes for all fields     
Output description:
1) P2_PAT_AGG_RESULT.csv is a result summary at Pattern level (PVinj, Oil allocation to each pattern, liq allocation to each pattern, iVRR, cumVRR)    


"""

#%%Importing libraries

import os
import sys
import fnmatch
import pandas as pd
import numpy as np
import geopandas as gpd
import tqdm
import time
from sklearn.linear_model import LinearRegression
from dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_columns', 1000)

#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput
import WFO_Functions as fn

#%% setting file and folder location
manual_data_folder= userinput.manualdata_input_folder
static_data_folder=userinput.staticdata_input_folder
output_folder=userinput.output_folder
input_folder = userinput.output_folder


wfo_result = userinput.wfo_result_output
pat_map =userinput.pattern_map_output 
pat_agg_output=userinput.pat_agg_output
masterxy1=userinput.wmaster_output
prod_inj_composite_id = userinput.prod_inj_output



wmaster_file="*_WMASTER.xls*"
sheettoread="Pattern_Attributes"

#%% Define standard column names as variables

composite_id = "COMPOSITE_ID"
pat_nm = "PAT_NAME"
field_res = "FIELD_RES_GROUP"
wb_sn="WELLBORE_SHORT_NAME"

datt1='MONTH_START_DATE'
field_list=userinput.field_list
#%%Defining Functions


def RF_Slope(pat_agg_df,years_offset):
    pat_agg_df_combined = pd.DataFrame()
    for prod in pat_agg_df.groupby(pat_nm): 
        df=pd.DataFrame(prod[1])
        start_date = max(df[datt1])-pd.offsets.DateOffset(years=years_offset)
        end_date = max(df[datt1]) 
        mask = (df[datt1] >= start_date) & (df[datt1] <= end_date)
        df1 = df.loc[mask]
        lr = LinearRegression()
        y_lr = df1["RF, %"].tolist()
        x_lr = df1['PVinj'].values.reshape(-1, 1)         
        try:
            lr.fit(x_lr, y_lr)
            df1["RF_Coef"] = lr.coef_[0]
        except:
            df1["RF_Coef"] = np.nan
            continue
        
        y_lr1 = df1["RF_PostInj, %"].tolist()
        x_lr1 = df1['PVinj'].values.reshape(-1, 1)         
        try:
            lr.fit(x_lr1, y_lr1)
            df1["RF_POSTINJ_Coef"] = lr.coef_[0]
        except:
            df1["RF_POSTINJ_Coef"] = np.nan
            continue   

        df1['AVG_RF'] = df1['RF, %'].mean()   
        df1['AVG_RF_POSTINJ'] = df1['RF_PostInj, %'].mean()
        pat_agg_df_combined = pd.concat([pat_agg_df_combined, df1])
        pat_agg_df_combined = pat_agg_df_combined.drop_duplicates(subset=[pat_nm], keep='last').reset_index(drop=True)
    return pat_agg_df_combined

def cum_slope(pat_agg_df,years_offset):
    pat_df_combined = pd.DataFrame()
    for pat,df_pat in pat_agg_df.groupby(pat_nm): 
        st_dt = max(df_pat[datt1])-pd.offsets.DateOffset(years=10)
        ed_dt = max(df_pat[datt1]) 
        mask1 = (df_pat[datt1] >= st_dt) & (df_pat[datt1] <= ed_dt)
        df1_temp = df_pat.loc[mask1]

        df2_temp = df1_temp[df1_temp["PAT_INJ_ALLOC"]>=df1_temp["HCPV"]*100]

        
        try:
            start_date = max(df2_temp[datt1])-pd.offsets.DateOffset(years=years_offset)
            end_date = max(df2_temp[datt1])
        except:
            print(df_pat[pat_nm].unique())
            start_date = max(df_pat[datt1])-pd.offsets.DateOffset(years=years_offset)
            end_date = max(df_pat[datt1])
        mask = (df2_temp[datt1] >= start_date) & (df2_temp[datt1] <= end_date)
        df1 = df2_temp.loc[mask]

        lr = LinearRegression()
        y_lr = df1["PAT_CUMOIL_ALLOC"].tolist()
        x_lr = df1["PAT_CUMINJ_ALLOC"].values.reshape(-1, 1)         
        try:
            lr.fit(x_lr, y_lr)
            df1["Cum_slope"] = lr.coef_[0]
        except:
            df1["Cum_slope"] = np.nan
            continue
        lr = LinearRegression()
        y_lr = df1["RF, %"].tolist()
        x_lr = df1['PVinj'].values.reshape(-1, 1)         
        try:
            lr.fit(x_lr, y_lr)
            df1["RF_Coef"] = lr.coef_[0]
        except:
            df1["RF_Coef"] = np.nan
            continue 
        y_lr1 = df1["RF_PostInj, %"].tolist()
        x_lr1 = df1['PVinj'].values.reshape(-1, 1)         
        try:
            lr.fit(x_lr1, y_lr1)
            df1["RF_POSTINJ_Coef"] = lr.coef_[0]
        except:
            df1["RF_POSTINJ_Coef"] = np.nan
            continue   


        # df1["CUMTotal_UG_WITHDRAW"]=((df1["PAT_CUMOIL_ALLOC"]*1.087)+(df1["PAT_CUMWATER_ALLOC"]*1.02))
        df1.rename(columns={"RF_Coef":"RFvPVinj_slope"},inplace=True)
        df1.rename(columns={"RF_POSTINJ_Coef":"RF_POSTINJvPVinj_slope"},inplace=True)
        df1.reset_index(drop=True, inplace=True)
        cummax_ug = df1.loc[df1[datt1].idxmax(),'PAT_CUMUGWITHDRAWL']
        cummin_ug = df1.loc[df1[datt1].idxmin(),'PAT_CUMUGWITHDRAWL']
        cummax_inj = df1.loc[df1[datt1].idxmax(),"PAT_CUMINJ_ALLOC"]
        cummin_inj = df1.loc[df1[datt1].idxmin(),"PAT_CUMINJ_ALLOC"]
        df1["CUMVRR_"+str(years_offset)+"_years"]=(cummax_inj-cummin_inj)/(cummax_ug-cummin_ug)
        ed_date1 = max(df1[datt1])
        df2 = df1[df1[datt1]>ed_date1-relativedelta(months=3)]

        df1["PAT_OIL_CD_ALLOC_3mon_avg"]=df2["PAT_OIL_CD_ALLOC"].mean()
        df1["PAT_WATER_CD_ALLOC_3mon_avg"]=df2["PAT_WATER_CD_ALLOC"].mean()
        df1["PAT_LIQ_CD_ALLOC_3mon_avg"]=df2["PAT_LIQ_CD_ALLOC"].mean()
        df1["PAT_INJ_CD_ALLOC_3mon_avg"]=df2["PAT_INJ_CD_ALLOC"].mean() 
        df1["PAT_UGWITHDRAWL_CD_3mon_avg"]=df2["PAT_UGWITHDRAWL_CD"].mean()

        pat_df_combined = pd.concat([pat_df_combined, df1])
        
        # pat_df_combined2=pat_df_combined1.groupby(composite_id).agg({"PAT_OIL_CD_ALLOC":"mean","PAT_WATER_CD_ALLOC":"mean","PAT_LIQ_CD_ALLOC":"mean","PAT_INJ_CD_ALLOC":"mean"})
        # pat_df_combined3=pat_df_combined.copy()
        # pat_df_combined3.columns = [x+"_3mon_avg" for x in pat_df_combined2.columns]
        # pat_df_combined3.reset_index(inplace=True)
        # for x in pat_df_combined2.columns:
        #     pat_df_combined[x]=pat_df_combined[composite_id].map(dict(zip(pat_df_combined3[composite_id],pat_df_combined3[x])))
        
        pat_df_combined = pat_df_combined.drop_duplicates(subset=[pat_nm], keep='last').reset_index(drop=True)
    return pat_df_combined

#%%%importing result file and pattern attributes

df_pat_att = pd.DataFrame()

for root,subdirs, file in os.walk(manual_data_folder):
    for filename in fnmatch.filter(file,wmaster_file):
        try:
            df=pd.read_excel(os.path.join(root,filename),sheet_name = sheettoread )
            df_pat_att = pd.concat([df_pat_att,df])
        except:
            continue

df_wfo_result1 = pd.read_csv(output_folder+"/"+wfo_result)
df_pat_map =pd.read_csv(output_folder+"/"+pat_map)

df_pat_maptemp=pd.DataFrame()
for pttemp, dfpttemp in df_pat_map.groupby([pat_nm]):
    dfpttemp["FIELD_NAME"].fillna(method='ffill',inplace=True)
    dfpttemp["RES_CODE"].fillna(method='ffill',inplace=True)
    dfpttemp[field_res].fillna(method="ffill",inplace=True)
    df_pat_maptemp=pd.concat([df_pat_maptemp,dfpttemp])

df_pat_map=df_pat_maptemp.copy()

dict_pat_map = dict(zip(df_pat_map[wb_sn],df_pat_map[pat_nm])) 

df_alloc_fac=pd.read_csv(output_folder+"/"+userinput.alloc_factor_output)
df_alloc_fac["COMID_PAT"]=df_alloc_fac[composite_id]+"_"+df_alloc_fac[pat_nm]
dict_alloc_fac=dict(zip(df_alloc_fac["COMID_PAT"],df_alloc_fac["AF"]))


df_wfo_result=df_wfo_result1.copy()
# df_wfo_result=df_wfo_result[df_wfo_result["FIELD_CODE"].isin(['QAHARIR', 'ZAULIYAH', 'SAIH RAWL'])]
#%%Defining contribution of oil, liquid and water production from each producer to respective pattern based on connectivity factor
df_pat_summary=pd.DataFrame()
# tqdm.tqdm.pandas()
# tq1 = tqdm.tqdm()
print("=====Getting Allocated Production Rate for each Pattern=====")
for fr, df_fr in df_wfo_result.groupby([field_res]):
    print(fr)
    tq = tqdm.tqdm(df_fr.groupby([composite_id]))
    for pr, df_pr in tq:
        df_pr_temp1=df_pr[~df_pr[pat_nm].isnull()]
        df_pr_temp2=df_pr[df_pr[pat_nm].isnull()]
        df_pr_temp2[pat_nm] = df_pr_temp2["s_"+wb_sn].map(dict_pat_map)
        df_pr1= pd.concat([df_pr_temp1,df_pr_temp2])
        
        for pt, df_pt in df_pr1.groupby([pat_nm]):
            # print(pt)
            try:
                af=dict_alloc_fac[pr+"_"+pt]
            except:
                af=0
            df_pt["PD_OIL_CONTRI_1week_avg"]=df_pt['C_OIL_PD_1week_avg']*af
            df_pt["PD_WAT_CONTRI_1week_avg"]=df_pt[ 'C_WATER_PD_1week_avg']*af
            df_pt["PD_LIQ_CONTRI_1week_avg"]=df_pt['C_LIQ_PD_1week_avg']*af

            df_pt["PD_OIL_CONTRI_3mon_avg"]=df_pt['C_OIL_PD_3mon_avg']*af
            df_pt["PD_WAT_CONTRI_3mon_avg"]=df_pt[ 'C_WATER_PD_3mon_avg']*af
            df_pt["PD_LIQ_CONTRI_3mon_avg"]=df_pt['C_LIQ_PD_3mon_avg']*af

            df_pt["CD_OIL_CONTRI_1week_avg"]=df_pt['C_OIL_CD_1week_avg']*af
            df_pt["CD_WAT_CONTRI_1week_avg"]=df_pt[ 'C_WATER_CD_1week_avg']*af
            df_pt["CD_LIQ_CONTRI_1week_avg"]=df_pt['C_LIQ_CD_1week_avg']*af

            df_pt["CD_OIL_CONTRI_3mon_avg"]=df_pt['C_OIL_CD_3mon_avg']*af
            df_pt["CD_WAT_CONTRI_3mon_avg"]=df_pt[ 'C_WATER_CD_3mon_avg']*af
            df_pt["CD_LIQ_CONTRI_3mon_avg"]=df_pt['C_LIQ_CD_3mon_avg']*af         

            df_pt["C_MAX_CUMOIL_CONTRI"]=df_pt['C_MAX_CUMOIL']*af
            df_pt["C_MAX_CUMWATER_CONTRI"]=df_pt['C_MAX_CUMWATER']*af
            df_pat_summary=pd.concat([df_pat_summary,df_pt])
#%%
df_pat_summary1=df_pat_summary[["s_"+wb_sn,"s_"+composite_id,wb_sn,composite_id,field_res,pat_nm,'PD_OIL_CONTRI_1week_avg', 'PD_WAT_CONTRI_1week_avg',
       'PD_LIQ_CONTRI_1week_avg','PD_OIL_CONTRI_3mon_avg', 'PD_WAT_CONTRI_3mon_avg',
       'PD_LIQ_CONTRI_3mon_avg','CD_OIL_CONTRI_1week_avg', 'CD_WAT_CONTRI_1week_avg',
       'CD_LIQ_CONTRI_1week_avg','CD_OIL_CONTRI_3mon_avg', 'CD_WAT_CONTRI_3mon_avg',
       'CD_LIQ_CONTRI_3mon_avg', 'C_MAX_CUMOIL_CONTRI','C_MAX_CUMWATER_CONTRI']]
df_pat_summary1=df_pat_summary1.reset_index(drop=True)


###Summarizing water injection rates for injector for each pattern
df_pat_summary_wi=pd.DataFrame()
print("=====Getting Allocated Injection Rate for each Pattern=====")
for fr, df_fr in df_wfo_result.groupby([field_res]):
    tq=tqdm.tqdm(df_fr.groupby(["s_"+composite_id]))
    for wi, df_wi in tq:
        df_pr_temp1=df_wi[~df_wi[pat_nm].isnull()]
        df_pr_temp2=df_wi[df_wi[pat_nm].isnull()]
        df_pr_temp2[pat_nm] = df_pr_temp2["s_"+wb_sn].map(dict_pat_map)
        df_wi1= pd.concat([df_pr_temp1,df_pr_temp2])
        for pt, df_pt in df_wi1.groupby([pat_nm]):
            # print(pt)
            # print(df_pt['CF_STATIC_I'].sum())
            try:
                af1=dict_alloc_fac[wi+"_"+pt]
            except:
                af1=0
            df_pt["PD_INJ_CONTRI_1week_avg"]=df_pt['C_INJ_PD_1week_avg'].mean()*af1
            df_pt["PD_INJ_CONTRI_3mon_avg"]=df_pt['C_INJ_PD_3mon_avg'].mean()*af1
            df_pt["CD_INJ_CONTRI_1week_avg"]=df_pt['C_INJ_CD_1week_avg'].mean()*af1
            df_pt["CD_INJ_CONTRI_3mon_avg"]=df_pt['C_INJ_CD_3mon_avg'].mean()*af1

            df_pt["C_CUM_INJ_CONTRI"]=df_pt['INJ_CUM'].mean()*af1
            df_pt["PROD_PAT_temp"]=df_pt[composite_id]+"_"+df_pt[pat_nm]
            df_pt["PROD_AF"]=df_pt["PROD_PAT_temp"].map(dict_alloc_fac)
            df_pt["Eff_Incl_Flag"]=df_pt.apply(lambda x: 1 if x["CF_STATIC_I"]>userinput.inj_cutoff_eff else 0, axis=1) ###make this cutoff as a user input
            df_pt["Eff_OIL_CD_3mon_avg"]=df_pt["C_OIL_CD_3mon_avg"]*df_pt["PROD_AF"]*df_pt["Eff_Incl_Flag"]
            df_pt["Eff_Total_OIL_CD_3mon_avg"]=df_pt["Eff_OIL_CD_3mon_avg"].sum()
            
            df_pat_summary_wi=pd.concat([df_pat_summary_wi,df_pt])


df_pat_summary_wi1=df_pat_summary_wi[["s_"+wb_sn,"s_"+composite_id,wb_sn,composite_id,field_res,pat_nm,"PD_INJ_CONTRI_1week_avg", "PD_INJ_CONTRI_3mon_avg", "CD_INJ_CONTRI_1week_avg", "CD_INJ_CONTRI_3mon_avg","C_CUM_INJ_CONTRI",'PROD_PAT_temp', 'PROD_AF', 'Eff_Incl_Flag', 'Eff_OIL_CD_3mon_avg',
       'Eff_Total_OIL_CD_3mon_avg']]
df_pat_summary_wi1=df_pat_summary_wi1.reset_index(drop=True)



df_pat_summ=pd.DataFrame()
print("=====Getting Pattern Level Aggregated Production Rate=====")
tq=tqdm.tqdm(df_pat_summary1.groupby([pat_nm]))
for pt, df_pt in tq:
    df_pt_temp = pd.DataFrame()
    # print(pt)
    # print(df_pt[field_res].unique())
    df_pt_temp[pat_nm]=[pt.strip()]
    df_pt_temp[field_res]=df_pt[field_res].unique()
    df_pt_temp["PD_PAT_OIL_1week"]=[df_pt['PD_OIL_CONTRI_1week_avg'].sum()]
    df_pt_temp["PD_PAT_WATER_1week"]=[df_pt['PD_WAT_CONTRI_1week_avg'].sum()]
    df_pt_temp["PD_PAT_LIQ_1week"]=[df_pt['PD_LIQ_CONTRI_1week_avg'].sum()]
    df_pt_temp["CD_PAT_OIL_1week"]=[df_pt['CD_OIL_CONTRI_1week_avg'].sum()]
    df_pt_temp["CD_PAT_WATER_1week"]=[df_pt['CD_WAT_CONTRI_1week_avg'].sum()]
    df_pt_temp["CD_PAT_LIQ_1week"]=[df_pt['CD_LIQ_CONTRI_1week_avg'].sum()]
    df_pt_temp["PD_PAT_OIL_3mon"]=[df_pt['PD_OIL_CONTRI_3mon_avg'].sum()]
    df_pt_temp["PD_PAT_WATER_3mon"]=[df_pt['PD_WAT_CONTRI_3mon_avg'].sum()]
    df_pt_temp["PD_PAT_LIQ_3mon"]=[df_pt['PD_LIQ_CONTRI_3mon_avg'].sum()]
    df_pt_temp["CD_PAT_OIL_3mon"]=[df_pt['CD_OIL_CONTRI_3mon_avg'].sum()]
    df_pt_temp["CD_PAT_WATER_3mon"]=[df_pt['CD_WAT_CONTRI_3mon_avg'].sum()]
    df_pt_temp["CD_PAT_LIQ_3mon"]=[df_pt['CD_LIQ_CONTRI_3mon_avg'].sum()]
    df_pt_temp["PAT_CUM_OIL"]=[df_pt['C_MAX_CUMOIL_CONTRI'].sum()]
    df_pt_temp["PAT_CUM_WATER"]=[df_pt['C_MAX_CUMWATER_CONTRI'].sum()]
    
    df_pat_summ=pd.concat([df_pat_summ,df_pt_temp])

df_pat_summ_wi=pd.DataFrame()
print("=====Getting Pattern Level Aggregated Injection Rate=====")
tq=tqdm.tqdm(df_pat_summary_wi1.groupby([pat_nm]))
for pt, df_pt in tq :
    df_pt_temp_wi = pd.DataFrame()
    # print(pt)
    # print(df_pt[field_res].unique())
    df_pt_temp_wi[pat_nm]=[pt.strip()]
    df_pt_temp_wi[field_res]=df_pt[field_res].unique()
    cid_len=len(df_pt["s_"+composite_id].unique())
    df_pt_temp_wi["PD_PAT_INJ_1week"]=[df_pt.drop_duplicates(subset=['s_'+composite_id], keep='first')['PD_INJ_CONTRI_1week_avg'].sum()]
    df_pt_temp_wi["PD_PAT_INJ_3mon"]=[df_pt.drop_duplicates(subset=['s_'+composite_id], keep='first')['PD_INJ_CONTRI_3mon_avg'].sum()]
    df_pt_temp_wi["CD_PAT_INJ_1week"]=[df_pt.drop_duplicates(subset=['s_'+composite_id], keep='first')['CD_INJ_CONTRI_1week_avg'].sum()]
    df_pt_temp_wi["CD_PAT_INJ_3mon"]=[df_pt.drop_duplicates(subset=['s_'+composite_id], keep='first')['CD_INJ_CONTRI_3mon_avg'].sum()]
    
    df_pt_temp_wi["PAT_CUM_INJ"]=[df_pt.drop_duplicates('s_'+composite_id, keep='first')["C_CUM_INJ_CONTRI"].sum()]
    df_pt_temp_wi["Eff_Total_OIL_CD_3mon_avg"]=[df_pt["Eff_Total_OIL_CD_3mon_avg"].mean()]
    df_pt_temp_wi["INJ_EFF_Pseudo"]=df_pt_temp_wi.apply(lambda x:x["Eff_Total_OIL_CD_3mon_avg"]/x["CD_PAT_INJ_3mon"] if x["CD_PAT_INJ_3mon"]>0 else 0,axis=1)

    df_pat_summ_wi=pd.concat([df_pat_summ_wi,df_pt_temp_wi])

df_pat_all =pd.merge(df_pat_summ,df_pat_summ_wi,on=[pat_nm,field_res],how="outer")
df_pat_all=pd.merge(df_pat_all,df_pat_att,on=[pat_nm],how="left")

df_pat_all["REMAINING_OIL"]= df_pat_all.apply(lambda x: 0.51*x["STOIIP"]-(x["PAT_CUM_OIL"]/1000000)-x["NO"]-x["RESERVES"] if x["PAT_LOC"]=="Crest" else 0.27*x["STOIIP"]-(x["PAT_CUM_OIL"]/1000000)-x["NO"]-x["RESERVES"], axis=1)
df_pat_all["Total_UG_WITHDRAW"]=((df_pat_all["CD_PAT_OIL_3mon"]*1.087)+(df_pat_all["CD_PAT_WATER_3mon"]*1.02))
df_pat_all["CUMTotal_UG_WITHDRAW"]=((df_pat_all["PAT_CUM_OIL"]*1.087)+(df_pat_all["PAT_CUM_WATER"]*1.02))

df_pat_all["iVRR"]=df_pat_all["CD_PAT_INJ_3mon"]/((df_pat_all["CD_PAT_OIL_3mon"]*1.087)+(df_pat_all["CD_PAT_WATER_3mon"]*1.02))
df_pat_all["CUM_VRR"]=df_pat_all["PAT_CUM_INJ"]/((df_pat_all["PAT_CUM_OIL"]*1.087)+(df_pat_all["PAT_CUM_WATER"]*1.02))
df_pat_all["PVinj"]=(df_pat_all["PAT_CUM_INJ"]/1000000)/df_pat_all["HCPV"]
df_pat_all["RF"]=((df_pat_all["PAT_CUM_OIL"]/1000000)/df_pat_all["STOIIP"])*100
df_pat_all["WCT"]=((df_pat_all["CD_PAT_WATER_3mon"])/(df_pat_all["CD_PAT_LIQ_3mon"]))*100
df_pat_all.reset_index(drop=True,inplace=True)


#%%Finding out PVinj and RF with time

df_prod_inj = pd.read_csv(output_folder+"/"+prod_inj_composite_id)
df_masterxy1 =pd.read_csv(output_folder+"/"+masterxy1)
df_masterxy1=df_masterxy1[df_masterxy1["FIELD_CODE"].isin(userinput.field_list)]
df_masterxy1=df_masterxy1[df_masterxy1["RES_CODE"].isin(userinput.reservoir_list)]
df_alloc_fac["C_WELL_TYPE"]=df_alloc_fac[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1["C_WELL_TYPE"])))
df_alloc_fac[field_res]=df_alloc_fac[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1[field_res])))

# from tqdm import tqdm
# length = sum(1 for row in open(output_folder+"/"+masterxy1, 'r'))
# with tqdm(total=length) as bar:
#     # do not skip any of the rows, but update the progress bar instead
#     dftemp11=pd.read_csv(output_folder+"/"+masterxy1, skiprows=lambda x: bar.update(1) and False)

df_prod_inj[wb_sn]=df_prod_inj[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1[wb_sn])))
# df_prod_inj=df_prod_inj[df_prod_inj["FIELD_NAME"].isin(userinput.field_list)]
# df_prod_inj=df_prod_inj[df_prod_inj["RES_CODE"].isin(userinput.reservoir_list)]
# df_prod_inj["STIR_FLAG"]=df_prod_inj[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1["STIR_FLAG"])))
# df_prod_inj["CRM_FLAG"]=df_prod_inj[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1["CRM_FLAG"])))


df_prod_inj=df_prod_inj[df_prod_inj["STIR_FLAG"]==1]
df_pat_dict1 = df_pat_map[[wb_sn,pat_nm]]
df_alloc_fac_ptmp=df_alloc_fac.rename(columns={"WELLID":wb_sn})
df_pat_dict2=df_alloc_fac_ptmp[[wb_sn,pat_nm]]
df_pat_dict=pd.concat([df_pat_dict1,df_pat_dict2])
df_pat_dict.drop_duplicates(inplace=True)
# dict_pat = dict((df_pat_map.groupby([wb_sn])[pat_nm].unique()))
# dict_pat = dict((df_alloc_fac.groupby(["WELLID"])[pat_nm].unique()))
dict_pat = dict((df_pat_dict.groupby([wb_sn])[pat_nm].unique()))

print("=======Calculating Pattern level production injection rate for each date===========")

df_prod_inj[pat_nm]=df_prod_inj[wb_sn].map(dict_pat)
df_prod_inj=df_prod_inj.explode(pat_nm)
df_prod_inj["PAT_CID_PAIR"]=df_prod_inj[composite_id]+"_"+df_prod_inj[pat_nm]

# df_pat_summary["PAT_CID_PAIR"]=df_pat_summary[composite_id]+"_"+df_pat_summary[pat_nm]
# b = dict(df_pat_summary.groupby(["PAT_CID_PAIR"])["CF_STATIC_P"].sum())
# df_prod_inj["CF_STATIC_P"]=df_prod_inj["PAT_CID_PAIR"].map(b)

# df_pat_summary_wi["PAT_CID_PAIR"]=df_pat_summary_wi["s_"+composite_id]+"_"+df_pat_summary_wi[pat_nm]
# a = dict(df_pat_summary_wi.groupby(["PAT_CID_PAIR"])["CF_STATIC_I"].sum())
# df_prod_inj["CF_STATIC_I"]=df_prod_inj["PAT_CID_PAIR"].map(a)

df_prod_inj["ALLOC_FAC"]=df_prod_inj["PAT_CID_PAIR"].map(dict_alloc_fac)
df_prod_inj["ALLOC_FAC"].fillna(0,inplace=True)
#normalising the allocation factor
# df_prod_inj["ALLOC_FAC"]= df_prod_inj["ALLOC_FAC"]/df_prod_inj.groupby("COMPOSITE_ID")["ALLOC_FAC"].transform('sum')

df_prod_inj["STOIIP"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["STOIIP"])))
df_prod_inj["HCPV"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["HCPV"])))
df_prod_inj["NO"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["NO"])))
df_prod_inj["RESERVES"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["RESERVES"])))
df_prod_inj["PAT_LOC"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["PAT_LOC"])))

df_prod_inj["Target iVRR"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["Target iVRR"])))
df_prod_inj["Target Pressure"]=df_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["Target Pressure"])))


col_list =[ 'MN_WATER_INJ','C_INJ_CD','C_CUMINJ','MN_PROD_WATER', 'MN_PROD_OIL','C_LIQUID','C_OIL_CD','C_WATER_CD', 'C_LIQ_CD', 'C_CUMOIL', 'C_CUMWATER', 'C_CUMLIQ']
for i in col_list:
    df_prod_inj[i+"_Alloc"]=df_prod_inj[i]*df_prod_inj["ALLOC_FAC"]
# col_list2 = ['MN_WATER_INJ','C_INJ_CD','C_CUMINJ']
# for i in col_list2:
#     df_prod_inj[i+"_Alloc"]=df_prod_inj[i]*df_prod_inj["ALLOC_FAC"]

df_prod_inj["COMPL_STATUS"]=df_prod_inj[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1["COMPL_STATUS"])))
df_prod_inj["C_WELL_TYPE"]=df_prod_inj[composite_id].map(dict(zip(df_masterxy1[composite_id],df_masterxy1["C_WELL_TYPE"])))

# df_prod_inj=df_prod_inj[df_prod_inj[field_res].isin(['QAHARIR_GHARIF',
#        'SAIH RAWL_SHUAIBA', 'ZAULIYAH_GHARIF'])]
#%%
# df_prod_inj = df_prod_inj[df_prod_inj['FIELD_NAME'].isin(['QAHARIR', 'SAIH RAWL', 'ZAULIYAH'])]
df_prod_inj.sort_values(by=[pat_nm, datt1])
df_pat_prod_inj=pd.DataFrame()
tq1 = tqdm.tqdm(df_prod_inj.groupby([pat_nm,datt1]))
for i, dfi in tq1:
    # print(i[0])
    df_pat_temp =pd.DataFrame()
    df_pat_temp[pat_nm]=[i[0]]
    df_pat_temp[datt1]=[i[1]]
    df_pat_temp["PAT_OIL_CD_ALLOC"]=[dfi['C_OIL_CD_Alloc'].sum()]
    df_pat_temp["PAT_WATER_CD_ALLOC"]=[dfi['C_WATER_CD_Alloc'].sum()]
    df_pat_temp["PAT_LIQ_CD_ALLOC"]=[dfi['C_LIQ_CD_Alloc'].sum()]

    df_pat_temp["PAT_OIL_ALLOC"]=[dfi['MN_PROD_OIL_Alloc'].sum()]
    df_pat_temp["PAT_WATER_ALLOC"]=[dfi['MN_PROD_WATER_Alloc'].sum()]
    df_pat_temp["PAT_LIQ_ALLOC"]=[dfi['C_LIQUID_Alloc'].sum()]
    df_pat_temp["PAT_INJ_ALLOC"]=[dfi['MN_WATER_INJ_Alloc'].sum()]
    # df_pat_temp["PAT_CUMOIL_ALLOC"]=[dfi['C_CUMOIL_Alloc'].sum()]
    # df_pat_temp["PAT_CUMWATER_ALLOC"]=[dfi['C_CUMWATER_Alloc'].sum()]
    # df_pat_temp["PAT_CUMLIQ_ALLOC"]=[dfi['C_CUMLIQ_Alloc'].sum()]
    df_pat_temp["PAT_INJ_CD_ALLOC"]=[dfi['C_INJ_CD_Alloc'].sum()]
    # df_pat_temp["PAT_CUMINJ_ALLOC"]=[dfi['C_CUMINJ_Alloc'].sum()]
    # print(dfi[field_res].unique())
    df_pat_temp[field_res]=dfi[field_res].unique()
    
    df_pat_prod_inj=pd.concat([df_pat_prod_inj,df_pat_temp])
df_pat_prod_inj.sort_values(by=[pat_nm, datt1])

df_pat_prod_inj[datt1]=pd.to_datetime(df_pat_prod_inj[datt1])
df_pat_prod_inj["C_DAYS_MONTH"]=df_pat_prod_inj[datt1].dt.days_in_month
# df_pat_prod_inj["PAT_OIL_ALLOC_1"]=df_pat_prod_inj["PAT_OIL_CD_ALLOC"]*df_pat_prod_inj["C_DAYS_MONTH"]

df_pat_prod_inj["PAT_CUMOIL_ALLOC"]=df_pat_prod_inj.groupby([pat_nm])["PAT_OIL_ALLOC"].cumsum()
df_pat_prod_inj["PAT_CUMWATER_ALLOC"]=df_pat_prod_inj.groupby([pat_nm])["PAT_WATER_ALLOC"].cumsum()
df_pat_prod_inj["PAT_CUMLIQ_ALLOC"]=df_pat_prod_inj.groupby([pat_nm])["PAT_LIQ_ALLOC"].cumsum()
df_pat_prod_inj["PAT_CUMINJ_ALLOC"]=df_pat_prod_inj.groupby([pat_nm])["PAT_INJ_ALLOC"].cumsum()

#%%

df_pat_prod_inj_temp = pd.DataFrame()
for p, dfp in df_pat_prod_inj.groupby([pat_nm]):
    dfptemp= pd.DataFrame()
    dfp1 = dfp[dfp["PAT_CUMINJ_ALLOC"]>0]
    dfp2 = dfp[dfp["PAT_CUMINJ_ALLOC"]==0]
    dfp1["PAT_CUMOIL_ALLOC_POSTINJ"]=dfp1["PAT_OIL_ALLOC"].cumsum()
    dfp1["PAT_CUMWATER_ALLOC_POSTINJ"]=dfp1["PAT_WATER_ALLOC"].cumsum()
    dfp1["PAT_CUMLIQ_ALLOC_POSTINJ"]=dfp1["PAT_LIQ_ALLOC"].cumsum()


    dfptemp=pd.concat([dfp1,dfp2])
    df_pat_prod_inj_temp=pd.concat([df_pat_prod_inj_temp,dfptemp])


# df_pat_prod_inj["PAT_CUMOIL_ALLOC_2"]=df_pat_prod_inj.groupby([pat_nm])["PAT_OIL_ALLOC_1"].cumsum()
# df_pat_prod_inj["PAT_CUMWATER_ALLOC"]=df_pat_prod_inj["PAT_CUMWATER_ALLOC"].cumsum()
# df_pat_prod_inj["PAT_CUMLIQ_ALLOC"]=df_pat_prod_inj["PAT_CUMLIQ_ALLOC"].cumsum()
# df_pat_prod_inj["PAT_CUMINJ_ALLOC_1"]=df_pat_prod_inj["PAT_CUMINJ_ALLOC"].cumsum()
#%%
df_pat_prod_inj=df_pat_prod_inj_temp.copy()

df_pat_prod_inj["STOIIP"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["STOIIP"])))
df_pat_prod_inj["HCPV"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["HCPV"])))
df_pat_prod_inj["NO"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["NO"])))
df_pat_prod_inj["RESERVES"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["RESERVES"])))
df_pat_prod_inj["PAT_LOC"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["PAT_LOC"])))

df_pat_prod_inj["REMAINING_OIL"]= df_pat_prod_inj.apply(lambda x: 0.51*x["STOIIP"]-(x["PAT_CUMOIL_ALLOC"]/1000000)-x["NO"]-x["RESERVES"] if x["PAT_LOC"]=="Crest" else 0.27*x["STOIIP"]-(x["PAT_CUMOIL_ALLOC"]/1000000)-x["NO"]-x["RESERVES"], axis=1)


# df_pat_prod_inj[field_res]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_map[pat_nm],df_pat_map[field_res])))
df_pat_prod_inj["RF, %"]=((df_pat_prod_inj["PAT_CUMOIL_ALLOC"]/1000000)/df_pat_prod_inj["STOIIP"])*100
df_pat_prod_inj["PVinj"]=(df_pat_prod_inj["PAT_CUMINJ_ALLOC"]/1000000)/df_pat_prod_inj["HCPV"]

df_pat_prod_inj["RF_PostInj, %"]=((df_pat_prod_inj["PAT_CUMOIL_ALLOC_POSTINJ"]/1000000)/df_pat_prod_inj["STOIIP"])*100


df_pat_prod_inj["PAT_UGWITHDRAWL_CD"]=((df_pat_prod_inj['PAT_OIL_CD_ALLOC']*1.087)+(df_pat_prod_inj['PAT_WATER_CD_ALLOC']*1.02))
ed_date1 = max(df_pat_prod_inj[datt1])
df1 = df_pat_prod_inj[df_pat_prod_inj[datt1]>ed_date1-relativedelta(months=3)]
dict_pat_oil_3mon_avg={}
dict_pat_wat_3mon_avg={}
dict_pat_liq_3mon_avg={}
dict_pat_UGW_3mon_avg={}
dict_pat_inj_3mon_avg={}

for pat_temp1,df2 in df1.groupby(pat_nm):
    dict_pat_oil_3mon_avg.update({pat_temp1:df2["PAT_OIL_CD_ALLOC"].mean()},ignore_index=True)
    dict_pat_wat_3mon_avg.update({pat_temp1:df2["PAT_WATER_CD_ALLOC"].mean()},ignore_index=True)
    dict_pat_liq_3mon_avg.update({pat_temp1:df2["PAT_LIQ_CD_ALLOC"].mean()},ignore_index=True)
    dict_pat_UGW_3mon_avg.update({pat_temp1:df2["PAT_UGWITHDRAWL_CD"].mean()},ignore_index=True)
    dict_pat_inj_3mon_avg.update({pat_temp1:df2["PAT_INJ_CD_ALLOC"].mean()},ignore_index=True)

df_pat_prod_inj["PAT_OIL_CD_ALLOC_3mon_avg"]=df_pat_prod_inj[pat_nm].map(dict_pat_oil_3mon_avg)
df_pat_prod_inj["PAT_WATER_CD_ALLOC_3mon_avg"]=df_pat_prod_inj[pat_nm].map(dict_pat_wat_3mon_avg)
df_pat_prod_inj["PAT_LIQ_CD_ALLOC_3mon_avg"]=df_pat_prod_inj[pat_nm].map(dict_pat_liq_3mon_avg)
df_pat_prod_inj["PAT_INJ_CD_ALLOC_3mon_avg"]=df_pat_prod_inj[pat_nm].map(dict_pat_inj_3mon_avg)
df_pat_prod_inj["PAT_UGWITHDRAWL_CD_3mon_avg"]=df_pat_prod_inj[pat_nm].map(dict_pat_UGW_3mon_avg)

df_pat_prod_inj["iVRR"]=df_pat_prod_inj.apply(lambda x: x[ "PAT_INJ_CD_ALLOC"]/x["PAT_UGWITHDRAWL_CD"] if x["PAT_UGWITHDRAWL_CD"]>0 else 0,axis=1)
df_pat_prod_inj["iVRR_3mon_avg"]=df_pat_prod_inj.apply(lambda x: x[ "PAT_INJ_CD_ALLOC_3mon_avg"]/x["PAT_UGWITHDRAWL_CD_3mon_avg"] if x["PAT_UGWITHDRAWL_CD_3mon_avg"]>0 else 0,axis=1)

df_pat_prod_inj["PAT_CUMUGWITHDRAWL"]=((df_pat_prod_inj['PAT_CUMOIL_ALLOC']*1.087)+(df_pat_prod_inj['PAT_CUMWATER_ALLOC']*1.02))
df_pat_prod_inj["CUM_VRR"]=df_pat_prod_inj['PAT_CUMINJ_ALLOC']/((df_pat_prod_inj['PAT_CUMOIL_ALLOC']*1.087)+(df_pat_prod_inj['PAT_CUMWATER_ALLOC']*1.02))
df_pat_prod_inj["FIELD_CODE"] =df_pat_prod_inj[field_res].apply(lambda x: x.split("_")[0])
df_pat_prod_inj["RES_CODE"] =df_pat_prod_inj[field_res].apply(lambda x: x.split("_")[1])

df_pat_prod_inj["Target iVRR"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["Target iVRR"])))
df_pat_prod_inj["Target Pressure"]=df_pat_prod_inj[pat_nm].map(dict(zip(df_pat_att[pat_nm],df_pat_att["Target Pressure"])))

#%%% Estimating pattern injection maturity score
from scipy import interpolate
df_pat_time = df_pat_prod_inj[['FIELD_CODE', 'RES_CODE', field_res, pat_nm, datt1,'PVinj','RF_PostInj, %']]

#pv_list = np.linspace(0, 2, 21)
pvi_list = [0.0, 0.05,0.1,0.15,0.2,0.25,0.5,0.75,1.0,1.25,1.50,1.75,2.0]


pat_df_output =pd.DataFrame()    
for fr, df_pat_time1 in df_pat_time.groupby(field_res): 
    #---------------------------------------------------           
    pat_list1 =[]
    pvi_snapsot_list1=[]
    for pat, pdf in df_pat_time1.groupby(pat_nm):
        pdf1 = pdf[pdf.PVinj==pdf.PVinj.max()]
        if len(pdf1)>0:
            pvi = pdf1["PVinj"].tolist()[0]
            if pvi>0:
                pvi_snapshot = max(list(filter(lambda x : x <= pvi, pvi_list)))
            else:
                pvi_snapshot = np.nan
            pat_list1.append(pat)
            pvi_snapsot_list1.append(pvi_snapshot)
    
    dict_pvi_snapshot = dict(zip(pat_list1,pvi_snapsot_list1))    
    df_pat_time1["pvi_snapshot"]  = df_pat_time1[pat_nm].map(dict_pvi_snapshot)  
    
    #---------------------------------------------------
    
    for pat, pdf in df_pat_time1.groupby(pat_nm):
        pvi_snapshot1 = pdf["pvi_snapshot"].max() 
        tmp_pat_list=[]
        tmp_pvi_list=[]
        tmp_rf_list=[]
        for pat1, pdf1 in df_pat_time1.groupby(pat_nm):    
            rf_function = interpolate.interp1d(pdf1['PVinj'].tolist(), pdf1['RF_PostInj, %'].tolist())
            if pvi_snapshot1<= pdf['PVinj'].max():
                try:
                    rf_snapshot = rf_function(pvi_snapshot1)[()]
                except:
                    rf_snapshot = np.nan
            else:
                rf_snapshot = np.nan
            tmp_pat_list.append(pat1)
            tmp_pvi_list.append(pvi_snapshot1)
            tmp_rf_list.append(rf_snapshot)
        tmp_df = pd.DataFrame()
        tmp_df["pvi_snapshot"] = tmp_pvi_list
        tmp_df["rf_snapshot"] = tmp_rf_list
        tmp_df["PVinj"] = pdf["PVinj"].max() 
        tmp_df["RF_PostInj, %"] = pdf[pdf["PVinj"]==pdf["PVinj"].max()]["RF_PostInj, %"].max()
        tmp_df[pat_nm] = pat
        tmp_df["ANALOGUE_PAT_NAME"] = tmp_pat_list

        tmp_df[field_res] = fr
        tmp_df["RF_score"] = tmp_df["rf_snapshot"].rank(pct = True)
        pat_df_output = pd.concat([pat_df_output,tmp_df])
       
pat_df_output1 = pat_df_output[pat_df_output.PAT_NAME==pat_df_output.ANALOGUE_PAT_NAME]    
del pat_df_output1["ANALOGUE_PAT_NAME"]




#%%

df_pat_all1=pd.DataFrame()
for pt1,dfpt1 in df_pat_prod_inj.groupby([pat_nm]):
    df_pt_temp=pd.DataFrame()
    df_pt_temp[pat_nm]=[pt1]
    end_dt = dfpt1[datt1].max()
    dfpt2=dfpt1[dfpt1[datt1]>=end_dt-relativedelta(months=3)]
    df_pt_temp["PAT_OIL_CD_ALLOC_3mon_avg"]=[dfpt2["PAT_OIL_CD_ALLOC"].mean()]
    df_pt_temp["PAT_WATER_CD_ALLOC_3mon_avg"]=[dfpt2["PAT_WATER_CD_ALLOC"].mean()]
    df_pt_temp["PAT_LIQ_CD_ALLOC_3mon_avg"]=[dfpt2["PAT_LIQ_CD_ALLOC"].mean()]
    df_pt_temp["PAT_INJ_CD_ALLOC_3mon_avg"]=[dfpt2["PAT_INJ_CD_ALLOC"].mean()]
    df_pt_temp["PAT_CUM_OIL"]=[dfpt2["PAT_CUMOIL_ALLOC"].max()]
    df_pt_temp["PAT_CUM_WATER"]=[dfpt2["PAT_CUMWATER_ALLOC"].max()]
    df_pt_temp["PAT_CUM_LIQ"]=[dfpt2["PAT_CUMLIQ_ALLOC"].max()]
    df_pt_temp["PAT_CUM_INJ"]=[dfpt2["PAT_CUMINJ_ALLOC"].max()]
    df_pt_temp["PAT_CUM_OIL_POSTINJ"]=[dfpt2["PAT_CUMOIL_ALLOC_POSTINJ"].max()]
    df_pat_all1=pd.concat([df_pat_all1,df_pt_temp])

df_pat_all1=pd.merge(df_pat_all1,df_pat_att,on=[pat_nm],how="left")

df_pat_all1["REMAINING_OIL"]= df_pat_all1.apply(lambda x: 0.51*x["STOIIP"]-(x["PAT_CUM_OIL"]/1000000)-x["NO"]-x["RESERVES"] if x["PAT_LOC"]=="Crest" else 0.27*x["STOIIP"]-(x["PAT_CUM_OIL"]/1000000)-x["NO"]-x["RESERVES"], axis=1)
df_pat_all1["Total_UG_WITHDRAW"]=((df_pat_all1["PAT_OIL_CD_ALLOC_3mon_avg"]*1.087)+(df_pat_all1["PAT_WATER_CD_ALLOC_3mon_avg"]*1.02))
df_pat_all1["CUMTotal_UG_WITHDRAW"]=((df_pat_all1["PAT_CUM_OIL"]*1.087)+(df_pat_all1["PAT_CUM_WATER"]*1.02))

df_pat_all1["iVRR"]=df_pat_all1["PAT_INJ_CD_ALLOC_3mon_avg"]/((df_pat_all1["PAT_OIL_CD_ALLOC_3mon_avg"]*1.087)+(df_pat_all1["PAT_WATER_CD_ALLOC_3mon_avg"]*1.02))
df_pat_all1["CUM_VRR"]=df_pat_all1["PAT_CUM_INJ"]/((df_pat_all1["PAT_CUM_OIL"]*1.087)+(df_pat_all1["PAT_CUM_WATER"]*1.02))
df_pat_all1["PVinj"]=(df_pat_all1["PAT_CUM_INJ"]/1000000)/df_pat_all1["HCPV"]
df_pat_all1["RF"]=((df_pat_all1["PAT_CUM_OIL"]/1000000)/df_pat_all1["STOIIP"])*100
df_pat_all1["RF_POSTINJ"]=((df_pat_all1["PAT_CUM_OIL_POSTINJ"]/1000000)/df_pat_all1["STOIIP"])*100
df_pat_all1["WCT"]=((df_pat_all1["PAT_WATER_CD_ALLOC_3mon_avg"])/(df_pat_all1["PAT_LIQ_CD_ALLOC_3mon_avg"]))*100
df_pat_all1["FIELD_NAME"]=df_pat_all1[pat_nm].map(dict(zip(df_pat_map[pat_nm],df_pat_map["FIELD_NAME"])))
df_pat_all1[field_res]=df_pat_all1[pat_nm].map(dict(zip(df_pat_map[pat_nm],df_pat_map[field_res])))

df_pat_all1.reset_index(drop=True,inplace=True)

#%%calculating slope

pat_agg_df_combined_3yr = RF_Slope(df_pat_prod_inj,3).rename(columns={'RF_Coef':'RF_Coef_3yr', 'AVG_RF': 'AVG_RF_3yr','RF_POSTINJ_Coef':'RF_POSTINJ_Coef_3yr', 'AVG_RF_POSTINJ': 'AVG_RF_POSTINJ_3yr'})
pat_agg_df_combined_5yr = RF_Slope(df_pat_prod_inj,5).rename(columns={'RF_Coef':'RF_Coef_5yr', 'AVG_RF': 'AVG_RF_5yr','RF_POSTINJ_Coef':'RF_POSTINJ_Coef_5yr', 'AVG_RF_POSTINJ': 'AVG_RF_POSTINJ_5yr'})
df_final = pat_agg_df_combined_3yr.merge(pat_agg_df_combined_5yr, left_on=pat_nm, right_on=pat_nm)
df_final= df_final[[pat_nm, 'RF_Coef_5yr','RF_Coef_3yr','RF_POSTINJ_Coef_5yr','RF_POSTINJ_Coef_3yr', 'AVG_RF_3yr','AVG_RF_5yr',"AVG_RF_POSTINJ_3yr","AVG_RF_POSTINJ_5yr" ]]


#%%%Calculating injection effciency


# pat_agg_df=df_pat_prod_inj
years_offset=3
df_eff = cum_slope(df_pat_prod_inj,years_offset)
dict_eff=dict(zip(df_pat_all[pat_nm],df_pat_all["INJ_EFF_Pseudo"]))
df_eff["INJ_EFF_Pseudo"] =df_eff[pat_nm].map(dict_eff) 
df_eff["INJ_EFF"]=df_eff.apply(lambda x: x["INJ_EFF_Pseudo"]*x["iVRR_3mon_avg"] if x["iVRR_3mon_avg"]<1 else x["INJ_EFF_Pseudo"],axis=1)

# df_eff["INJ_EFF % (Capped)"] = df_eff.apply(lambda z: 2 if (z["INJ_EFF"]*100)>2 else z["INJ_EFF"]*100,axis=1)

df_eff["FIELD_CODE"] =df_eff[field_res].apply(lambda x: x.split("_")[0])
df_eff["RES_CODE"] =df_eff[field_res].apply(lambda x: x.split("_")[1])

df_eff["INJ_EFF_PERCENTILE"]=df_eff.groupby([field_res])["INJ_EFF"].rank(pct=True)
df_eff["PAT_OILCUT"]= df_eff["PAT_OIL_CD_ALLOC_3mon_avg"]/df_eff["PAT_LIQ_CD_ALLOC_3mon_avg"]
df_eff["PAT_WATERCUT"]= df_eff["PAT_WATER_CD_ALLOC_3mon_avg"]/df_eff["PAT_LIQ_CD_ALLOC_3mon_avg"]


df_eff_temp=df_eff[[pat_nm,"RFvPVinj_slope","RF_POSTINJvPVinj_slope","INJ_EFF",field_res]]
df_eff_temp1=pd.DataFrame()
for rg, dfrg in df_eff_temp.groupby([field_res]):
    dfrg["RFvPVinj_slope_Score"]=dfrg["RF_POSTINJvPVinj_slope"].rank(pct = True)
    dfrg["INJ_EFF_Score"]=dfrg["INJ_EFF"].rank(pct = True)
    df_eff_temp1=pd.concat([df_eff_temp1,dfrg])

df_eff["RFvPVinj_slope_Score"]=df_eff[pat_nm].map(dict(zip(df_eff_temp1[pat_nm],df_eff_temp1["RFvPVinj_slope_Score"])))
df_eff["INJ_EFF_Score"]=df_eff[pat_nm].map(dict(zip(df_eff_temp1[pat_nm],df_eff_temp1["INJ_EFF_Score"])))

# df_eff["Target_iVRR"] = df_eff.apply(lambda x: 1.2 if x["PAT_LOC"]=="Crest" else 1 if x["FIELD_CODE"]!="SAIH RAWL" else 0.9,axis=1)
# df_eff["New_iVRR"]=df_eff.apply(lambda x: x["Target_iVRR"] if x["Current_iVRR"]<x["Target_iVRR"] else x["Current_iVRR"],axis=1)

# df_eff["EXPECTED_INJ_CD"]=df_eff['PAT_UGWITHDRAWL_CD_3mon_avg']*df_eff["New_iVRR"]
# df_eff_temp1=df_eff[df_eff['PAT_INJ_CD_ALLOC_3mon_avg']==0]
# df_eff_temp2=df_eff[df_eff['PAT_INJ_CD_ALLOC_3mon_avg']!=0]
# df_eff_temp2["PROPOSED_INJ_CD"]= df_eff_temp2.apply(lambda x:x["EXPECTED_INJ_CD"] if ((x["EXPECTED_INJ_CD"]-x['PAT_INJ_CD_ALLOC_3mon_avg'])*100/x['PAT_INJ_CD_ALLOC_3mon_avg'])<20 else x['PAT_INJ_CD_ALLOC_3mon_avg']*1.20,axis=1)
# df_eff=pd.concat([df_eff_temp1,df_eff_temp2])
# df_eff["DELTA_INJ_CD"]=df_eff["PROPOSED_INJ_CD"]-df_eff['PAT_INJ_CD_ALLOC_3mon_avg']
# df_eff["DELTA_INJ_CD %"]=(df_eff["PROPOSED_INJ_CD"]-df_eff['PAT_INJ_CD_ALLOC_3mon_avg'])*100/df_eff['PAT_INJ_CD_ALLOC_3mon_avg']


# df_eff["POTENTIAL_OIL_GAIN_CD"]=df_eff["DELTA_INJ_CD"]*df_eff["INJ_EFF"]
# df_eff["New_Oil_CD"]=df_eff['PAT_OIL_CD_ALLOC_3mon_avg']+df_eff["POTENTIAL_OIL_GAIN_CD"]
# df_eff["POTENTIAL_GAIN_iVRR_CONTRAINED %"]=(df_eff["POTENTIAL_OIL_GAIN_CD"])*100/df_eff['PAT_OIL_CD_ALLOC_3mon_avg']

df_eff_com=df_eff.copy()

#%%%Calculating avg pattern pressure and Integrating pressure with other pattern attributes
# Avg pattern pressure is weighted avegrage of allocation factor of all the wells that had pressure recording in last 1 year for thta pattern 
df_press= pd.read_csv(output_folder+"/"+"P1_Static_Pressure_Data_Analytics.csv")
df_press["Date"]=pd.to_datetime(df_press["Date"])
df_press["Cutoff_Date"]= (max(df_press["Date"])-relativedelta(years=2))
df_press["REF_Date"]= pd.to_datetime(max(df_prod_inj[datt1]))
df_press["delta"]= df_press.apply(lambda x:relativedelta(x["Date"],x["Cutoff_Date"]),axis=1)
df_press["delT"]=df_press["delta"].apply(lambda x: x.months+(x.years*12))
df_press["n_delT"]=df_press["delT"]/24
df_press1 = df_press[df_press["Date"]>(max(df_press["Date"])-relativedelta(years=2))]
df_press1[pat_nm]=df_press1[wb_sn].map(dict_pat)
df_press1=df_press1.explode(pat_nm)

df_press1["PAT_CID_PAIR"]=df_press1[composite_id]+"_"+df_press1[pat_nm]
df_press1["ALLOC_FAC"]=df_press1["PAT_CID_PAIR"].map(dict_alloc_fac)
df_press1["ALLOC_FAC*n_delT"]=df_press1["ALLOC_FAC"]*df_press1["n_delT"]
# df_press1["ALLOC_FAC"].fillna(0,inplace=True)
df_press1['n_AF']= df_press1["ALLOC_FAC"]/ df_press1.groupby(pat_nm)["ALLOC_FAC"].transform('sum')
df_press1["WtPress"] =df_press1['n_AF']*df_press1["Datum_Pressure"]
dict_wtavgpress=dict(df_press1.groupby(pat_nm)["WtPress"].sum())
dict_prs_conf_fac=dict(df_press1.groupby(pat_nm)["ALLOC_FAC*n_delT"].sum())
df_press1["Pat_Wt_Avg_Press"]=df_press1[pat_nm].map(dict_wtavgpress)
df_press1["Pat_Wt_Avg_Press"] = df_press1["Pat_Wt_Avg_Press"].apply(lambda x : np.nan if x==0 else x)
df_press1["Pat_Press_Conf_Fac"]=df_press1[pat_nm].map(dict_prs_conf_fac)

#%%
df_eff_com["RF_score"]=df_eff_com[pat_nm].map(dict(zip(pat_df_output1[pat_nm],pat_df_output1["RF_score"])))
df_eff_com["Pat_Wt_Avg_Press"]=df_eff_com[pat_nm].map(dict_wtavgpress)
df_eff_com["Pat_Wt_Avg_Press"] = df_eff_com["Pat_Wt_Avg_Press"].apply(lambda x : np.nan if x==0 else x)
df_eff_com["Pat_Press_Conf_Fac"]=df_eff_com[pat_nm].map(dict_prs_conf_fac)
#%%
df_pat_map1 =df_pat_map[[pat_nm, wb_sn, 'ALLOC_FAC', field_res]]
dict_compid=dict((df_alloc_fac.groupby(["WELLID"])[composite_id].unique()))
df_pat_map1[composite_id]=df_pat_map1[wb_sn].map(dict_compid)
df_pat_map1=df_pat_map1.explode(composite_id)
df_alloc_fac1 = df_alloc_fac[[pat_nm, 'WELLID', 'AF',composite_id,field_res]]


df_alloc_all=pd.merge(df_alloc_fac1,df_pat_map1, left_on=[field_res,pat_nm,composite_id],right_on=[field_res,pat_nm,composite_id],how="outer")
df_alloc_all["PAT_WID"]=df_alloc_all[pat_nm]+"_"+df_alloc_all['WELLID']
df_alloc_all["PAT_WBSN"]=df_alloc_all[pat_nm]+"_"+df_alloc_all[wb_sn]
df_alloc_all.dropna(how="all",inplace=True)


#%%Getting Pattern Area
df_pat_shp = gpd.GeoDataFrame.from_file(output_folder +"/Pat_combined.shp")
df_pat_shp["PAT_AREA"]=df_pat_shp["geometry"].area
pat_area_dict= dict(zip(df_pat_shp[pat_nm],df_pat_shp["PAT_AREA"]))
#%% Getting pattern producer injector total allocation factor for open and all wells

df_pat_prod_wells=pd.DataFrame()
df_pat_inj_wells=pd.DataFrame()
for i , dfi in df_prod_inj.groupby(pat_nm):
#no. of all and active allocated producers
    dfi_prod=dfi[dfi["C_WELL_TYPE"]=="OP"]
    dfi_prod=dfi_prod[dfi_prod[datt1]==dfi_prod[datt1].max()]
    dfi_prod["ALLOC_PRODUCERS_TOTAL"]=dfi_prod["ALLOC_FAC"].sum()
    dfi_prod1=dfi_prod[dfi_prod['COMPL_STATUS']=="OPEN"]
    dfi_prod["ALLOC_PRODUCERS_ACTIVE"]=dfi_prod1["ALLOC_FAC"].sum()
    dfi_prod_temp=dfi_prod[[pat_nm, "ALLOC_PRODUCERS_TOTAL","ALLOC_PRODUCERS_ACTIVE"]]
    dfi_prod_temp=dfi_prod_temp.drop_duplicates()
    df_pat_prod_wells=pd.concat([df_pat_prod_wells,dfi_prod_temp])

#no. of all and active allocated injectors
    dfi_inj=dfi[dfi["C_WELL_TYPE"].isin(['OP-WI', 'WI'])]
    dfi_inj=dfi_inj[dfi_inj[datt1]==dfi_inj[datt1].max()]
    dfi_inj["ALLOC_INJECTORS_TOTAL"]=dfi_inj["ALLOC_FAC"].sum()
    dfi_inj1=dfi_inj[dfi_inj['COMPL_STATUS']=="OPEN"]
    dfi_inj["ALLOC_INJECTORS_ACTIVE"]=dfi_inj1["ALLOC_FAC"].sum()
    dfi_inj_temp=dfi_inj[[pat_nm, "ALLOC_INJECTORS_TOTAL","ALLOC_INJECTORS_ACTIVE"]]
    dfi_inj_temp=dfi_inj_temp.drop_duplicates()
    df_pat_inj_wells=pd.concat([df_pat_inj_wells,dfi_inj_temp])

df_pat_allocated_wells=pd.merge(df_pat_prod_wells,df_pat_inj_wells,on=pat_nm,how="outer")
dict_pat_total_prod=dict(zip(df_pat_prod_wells[pat_nm], df_pat_prod_wells["ALLOC_PRODUCERS_TOTAL"]))
dict_pat_active_prod=dict(zip(df_pat_prod_wells[pat_nm], df_pat_prod_wells["ALLOC_PRODUCERS_ACTIVE"]))
dict_pat_total_inj=dict(zip(df_pat_inj_wells[pat_nm], df_pat_inj_wells["ALLOC_INJECTORS_TOTAL"]))
dict_pat_active_inj=dict(zip(df_pat_inj_wells[pat_nm], df_pat_inj_wells["ALLOC_INJECTORS_ACTIVE"]))

#%%Adding pattern area, allocated wells to pat_rank_gain_output

df_eff_com["PAT_AREA"]=df_eff_com[pat_nm].map(pat_area_dict)
df_eff_com["ALLOC_PRODUCERS_TOTAL"]=df_eff_com[pat_nm].map(dict_pat_total_prod)
df_eff_com["ALLOC_PRODUCERS_ACTIVE"]=df_eff_com[pat_nm].map(dict_pat_active_prod)
df_eff_com["ALLOC_INJECTORS_TOTAL"]=df_eff_com[pat_nm].map(dict_pat_total_inj)
df_eff_com["ALLOC_INJECTORS_ACTIVE"]=df_eff_com[pat_nm].map(dict_pat_active_inj)

# %%
#Output writing in required output folder
# output_folder_temp = "C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1"

df_prod_inj.to_csv(output_folder+"/"+"P2_PROD_INJ_COMPOSITEID_ALLOCATED_ANALYTICS.csv",index=False)

df_pat_all1.to_csv(output_folder+"/"+pat_agg_output,index=False)

df_pat_prod_inj.to_csv(output_folder+"/"+userinput.pat_time_agg,index=False)

df_final.to_csv(output_folder+"/P2_PAT_AGG_RF_SLOPE.csv", index=False)

df_eff_com.to_csv(output_folder+"/"+userinput.pat_rank_gain_output,index=False)
df_alloc_all.to_csv(output_folder+"/P2_All_Alloc_Fac.csv",index=False)

print("*********************************************************************************")
print("Exported:"+userinput.pat_agg_output )
print("Exported:"+userinput.pat_time_agg )
print("Exported:"+userinput.pat_rank_gain_output )
print("*********************************************************************************")

#%%
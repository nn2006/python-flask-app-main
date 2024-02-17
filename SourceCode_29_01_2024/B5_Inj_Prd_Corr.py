# -*- coding: utf-8 -*-
"""
Created on Sat Nov 27 07:45:59 2021

@author: Manu.Ujjwal
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 25 19:55:12 2021

@author: MUjjwal
"""

#%% 1. IMPORT PYTHON LIBRARIES
#=================================================================================================
import os
import sys
import __main__
import pandas as pd
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from scipy import signal as ss
from dateutil.relativedelta import*
import tqdm
import geopandas as gpd
import numpy as np
 
pd.set_option('display.max_columns', 40)
pd.options.mode.chained_assignment = None
#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput

#%% setting file and folder location

# dynamic_data_folder = userinput.dynamicdata_input_folder+"/SR"
manual_data_folder= userinput.manualdata_input_folder
static_data_folder=userinput.staticdata_input_folder
output_folder=userinput.output_folder
input_folder = userinput.output_folder

area_shpfile = "01_InjDrainage_DistMatrix_Shapefile.shp"
pattern_file=userinput.pattern_map_output
pat_dist=userinput.pattern_dist_matrix
#%%
prdinj_file = userinput.prod_inj_output
dist_matrix_file =userinput.dist_matrix_output

#current script filename
#pyfilename = (__main__.__file__).split("/")[-1]

#%% Defining columns headers as variables

field_code="FIELD_CODE"
composite_id="COMPOSITE_ID"
w_type="C_WELL_TYPE"
compl_mid_x="COMPL_MID_X"
compl_mid_y="COMPL_MID_Y"
compl_start="COMPL_START"
compl_status="COMPL_STATUS"
zn_close="ZONE_CLOSED"
zn_open="ZONE_OPEN"
compl_mid_md="COMPL_MID_MD"

dat="DATES"
datt1="MONTH_START_DATE"
field_res="FIELD_RES_GROUP"
v_dist="DISTANCE"
inj_pd ="C_INJ_PD"
wb_sn='WELLBORE_SHORT_NAME'

#%% 2. DEFINE FOLDER PATHS AND DATA TABLE NAMES
#=================================================================================================

prdinj_df = pd.read_csv(input_folder+"/"+prdinj_file)
prdinj_df=prdinj_df[prdinj_df["STIR_FLAG"]==1]
# prdinj_df=prdinj_df[prdinj_df["FIELD_NAME"].str.contains("|".join(userinput.field_list),regex=True,na=False)]
# prdinj_df=prdinj_df[prdinj_df["RES_CODE"].str.contains("|".join(userinput.reservoir_list),regex=True,na=False)]




dist_df = pd.read_csv(input_folder+"/"+dist_matrix_file)

# df_rft1=df_rft1[df_rft1["Run Date"]>=pd.to_datetime("1/1/2016 00:00:00")]
prdinj_df[dat]=prdinj_df[datt1]
prdinj_df[dat]=pd.to_datetime(prdinj_df[dat])
prdinj_df[datt1]=pd.to_datetime(prdinj_df[datt1])
prdinj_df=prdinj_df[prdinj_df[dat]>=(pd.to_datetime("1/1/2016 00:00:00"))]

#Assumptions - Distance cutoff
dict_dist_cutoff = userinput.dict_dist_cutoff

df_pat_dist=pd.read_csv(input_folder+"/"+pat_dist)
df_area = gpd.GeoDataFrame.from_file(input_folder+"/"+area_shpfile)
# df_area[field_res1]=df_area['s_WB_SHORT'].map(dict(zip(df_wmaster_com["WB_SHORTNAME"],df_wmaster_com[field_res1])))
df_area = df_area[df_area["DrainageRa"]==userinput.area_drainage_radius]
df_area=df_area.rename(columns={"s_WELLBORE":"s_"+wb_sn,
                                "s_COMPOSIT":"s_"+composite_id,"WELLBORE_S":wb_sn,
                                "COMPOSITE_":composite_id})
# df_area["WP"]=df_area['s_WB_SHORT']+"_"+df_area['WB_SHORTNA']

df_area["WP"]=df_area['s_'+wb_sn]+"_"+df_area[wb_sn]

#%% 3. LOAD DATA INTO PYTHON
#=================================================================================================


prd_df = prdinj_df[prdinj_df[w_type] == "OP"].reset_index(drop=True)
inj_df = prdinj_df[prdinj_df[w_type] == "WI"].reset_index(drop=True)

# dist_df ["Dist_Cutoff"]= dist_df[field_res].map(dict_dist_cutoff)

#dist_df_wi_op = dist_df_wi_op[dist_df_wi_op["FIELD_RES"].isin(FIELD_RES_LIST)]

#%%Defining distance flag

dist_df["WP"]=dist_df['s_'+wb_sn]+"_"+dist_df[wb_sn]
dist_df["OLP_AREA"]=dist_df["WP"].map(dict(zip(df_area["WP"],df_area["Area"])))
dist_df["OLP_AREA"]=dist_df["OLP_AREA"].fillna(0)
dist_df["PAT_NAME"]=dist_df["WP"].map(dict(zip(df_pat_dist["WELL_PAIR_SN"],df_pat_dist["PAT_NAME"])))
dist_df["CONN_OPEN_INJ"]=dist_df["WP"].map(dict(zip(df_pat_dist["WELL_PAIR_SN"],df_pat_dist["CONN_OPEN_INJ"])))
dist_df.drop(columns=["WP"],inplace=True)
dist_df["DIST_CUTOFF"] = dist_df[field_res].map(userinput.dict_dist_cutoff)
dist_df["ZMS_CUTOFF"] = dist_df[field_res].map(userinput.dict_zms_cutoff)
dist_df["DIST_FLAG_1"] =  dist_df.apply(lambda x: 1 if x["DISTANCE"]<= x["DIST_CUTOFF"] else 0 , axis=1 )
dist_df1=dist_df[dist_df['FIELD_CODE']=="SAIH RAWL"]
dist_df2=dist_df[dist_df['FIELD_CODE']!="SAIH RAWL"]
dist_df1["DIST_FLAG_1"] =  dist_df1.apply(lambda x: 1 if x["OLP_AREA"]>0 else 0 , axis=1 )
dist_df=pd.concat([dist_df1,dist_df2])
dist_df3=pd.DataFrame()
#making dist_flag 1 for wells within same pattern
for comp_id, dfcompid in dist_df.groupby([composite_id]):

    dfcompid.reset_index(inplace=True, drop=True)
    dfcompid1=dfcompid[~dfcompid["CONN_OPEN_INJ"].isnull()]
    try:
        dfcompid1["DIST_FLAG"]=dfcompid1.apply(lambda r: 1 if r['s_'+wb_sn] in r["CONN_OPEN_INJ"] else r["DIST_FLAG_1"],axis=1 )
    except:
        dfcompid1["DIST_FLAG"]=dfcompid1["DIST_FLAG_1"]
    dfcompid2=dfcompid[dfcompid["CONN_OPEN_INJ"].isnull()]
    dfcompid2["DIST_FLAG"]=dfcompid2["DIST_FLAG_1"]
    dfcompid3=pd.concat([dfcompid1,dfcompid2])
    dist_df3=dist_df3.append(dfcompid3)


dist_df=dist_df3.copy()
dist_df_wi_op = dist_df[dist_df.WELL_PAIR_TYPE=='WI_OP']
dist_df_wi_op.drop_duplicates(inplace=True)

#%% Function to calculate correlation factor between producer and injector paramaters

def calc_corr_factor(pwell, pwell_prd_df, prd_param_col, inj_list, inj_df,inj_param_col, med_filt_kernel= 5):
    df_prd_corr = pd.DataFrame()
    df_corr_inpdata = pd.DataFrame()
    if len(pwell_prd_df)>1:
        pdf = pwell_prd_df.copy()
        pdf[dat] = pd.to_datetime(pdf[dat])
        pdf = pdf[[dat,prd_param_col]]
        pdf[dat] = pdf.apply(lambda x: x[dat].date(), axis=1)
        pdf.sort_values([dat], ascending =True, inplace=True)               
        pdf_startdate = pdf[dat].min()
        pdf[prd_param_col].fillna(0, inplace=True)
        
        for iwell in inj_list:
            idf = inj_df[inj_df.COMPOSITE_ID==iwell]
            idf.sort_values([dat], ascending =True, inplace=True)
            idf[inj_param_col].fillna(0, inplace=True)
            idf["temp_cum"] = idf[inj_param_col].cumsum()
            idf =idf[idf.temp_cum>0]
            del idf["temp_cum"]            
            idf[dat] = pd.to_datetime(idf[dat])    
            if len(idf)>0:
                idf[dat] = idf.apply(lambda x: x[dat].date(), axis=1)                
                idf = idf[[dat,inj_param_col]]            
                idf_startdate = idf[dat].min()
                
                startdate = max(pdf_startdate, idf_startdate) - relativedelta(days=90)
                enddate =  min(pdf[dat].max(), idf[dat].max())
                
                df0 = pd.merge(pdf, idf, on =dat, how ="outer")
                df0 = df0[df0[dat]>=startdate]
                df0 = df0[df0[dat]<=enddate]
                df = df0[[prd_param_col,inj_param_col]]
                df.fillna(0, inplace=True)
                # print(df)
                if len(df) >10:
                    df[inj_param_col] = ss.medfilt(df[inj_param_col],med_filt_kernel)
                    df[prd_param_col] = ss.medfilt(df[prd_param_col],med_filt_kernel)
                    
                    #Normalize all the features using Min Max scaler               
                    scaler = MinMaxScaler()
                    df1 =df.copy()
                    # print(pwell)
                   
                    for col in df.columns:
                        df1[col] = scaler.fit_transform(df[col].values.reshape(-1,1).tolist())

                    corrMatrix = df1.corr()
                    df_corr1 = corrMatrix.reset_index()
                    df_corr1 = df_corr1[df_corr1["index"] !=inj_param_col]
    
                    df_corr1[composite_id] = pwell
                    df_corr1["s_"+composite_id] = iwell
                    df_corr1["StartDate"] = startdate
                    df_corr1["EndDate"] = enddate
                    df_corr1 = df_corr1.rename(columns ={inj_param_col:"CORR_FAC"})
                    del df_corr1["index"]
                    del df_corr1[prd_param_col]
                
                    lr = LinearRegression()
                    y_lr = df1[prd_param_col].tolist()
                    x_lr = df1[inj_param_col].values.reshape(-1, 1)         
                    lr.fit(x_lr, y_lr)
                    dict_lr_coef = dict(zip(inj_list,lr.coef_))     
            
                    df_corr1["LR_Coef"] = lr.coef_[0]
                    df_corr1["LR_R2"] = lr.score(x_lr, y_lr)
                    
                    df_corr1["PRD_PARAM"] = prd_param_col
                    df_corr1["INJ_PARAM"] = inj_param_col 
                    df_corr1["CORR_BASIS"] = prd_param_col+"/"+inj_param_col  
                    df_prd_corr = df_prd_corr.append(df_corr1)
                    
                    df[composite_id] = pwell
                    df["s_"+composite_id] =iwell
                    df["CORR_BASIS"] = prd_param_col+"/"+inj_param_col                    
                    df_corr_inpdata = df_corr_inpdata.append(df)

                    df_corr_inpdata["CORR_BASIS"] = prd_param_col+"/"+inj_param_col
                    
                    
                    # print("Completed :", prd_param_col+"/"+inj_param_col, pwell+"/"+iwell, pdf_startdate, idf_startdate, startdate)
    return df_prd_corr, df_corr_inpdata

#%% CREATE CORR FACTOR DATAFRAME
#-------------------------------------------------------------------------------------------------
df_prd_corr_wct = pd.DataFrame()
df_prd_corr_liq = pd.DataFrame()
df_prd_corr_wat = pd.DataFrame()

df_prdinj_corr_inpdata = pd.DataFrame()
tq1= tqdm.tqdm(prd_df.groupby(composite_id))
for pwell, pdf0 in tq1:    
    
    pdf0["C_LIQ_PD"]=pdf0["C_LIQ_PD"].replace(np.inf, 0)
    pdf0["C_LIQ_PD"]=pdf0["C_LIQ_PD"].replace(-np.inf, 0)

    pdf0["C_WATER_PD"]=pdf0["C_WATER_PD"].replace(np.inf, 0)
    pdf0["C_WATER_PD"]=pdf0["C_WATER_PD"].replace(-np.inf, 0)

    pdf0["C_WCT_PD"]=pdf0["C_WCT_PD"].replace(np.inf, 0)
    pdf0["C_WCT_PD"]=pdf0["C_WCT_PD"].replace(-np.inf, 0)

    ddf =dist_df_wi_op[dist_df_wi_op.COMPOSITE_ID==pwell]              
    ddf=ddf[ddf["DIST_FLAG"]==1]   
    inj_list= ddf["s_"+composite_id].tolist()  
    
    #WCT vs Injection correlation
    df_corr_pwell_wct = calc_corr_factor(pwell, pdf0, "C_WCT_PD", inj_list, inj_df,inj_pd,med_filt_kernel= 5)[0]
    df_prd_corr_wct = df_prd_corr_wct.append(df_corr_pwell_wct)
    
    #Liqrate vs Injection correlation
    df_corr_pwell_liq = calc_corr_factor(pwell, pdf0, "C_LIQ_PD", inj_list, inj_df,inj_pd,med_filt_kernel= 5)[0]
    df_prd_corr_liq = df_prd_corr_liq.append(df_corr_pwell_liq)

    #Waterrate vs Injection correlation
    df_corr_pwell_wat = calc_corr_factor(pwell, pdf0, "C_WATER_PD", inj_list, inj_df,inj_pd,med_filt_kernel= 5)[0]
    df_prd_corr_wat = df_prd_corr_wat.append(df_corr_pwell_wat)
    
    #Add rawdata used in corr calc in a separate dataframe
    df_corr_inp_liq = calc_corr_factor(pwell, pdf0, "C_LIQ_PD", inj_list, inj_df,inj_pd,med_filt_kernel= 5)[1]
    df_corr_inp_wat = calc_corr_factor(pwell, pdf0, "C_WATER_PD", inj_list, inj_df,inj_pd,med_filt_kernel= 5)[1]
    df_corr_inp_wct = calc_corr_factor(pwell, pdf0, "C_WCT_PD", inj_list, inj_df,inj_pd,med_filt_kernel= 5)[1]
    df_prdinj_corr_inpdata  = df_prd_corr_wat.append([df_corr_inp_liq,df_corr_inp_wat,df_corr_inp_wct])

#%%
##Limit liq correlation to zero, replace negative correlation values with zero
df_prd_corr_liq["CORR_FAC"]=df_prd_corr_liq["CORR_FAC"].apply(lambda x:x if x>0 else 0)
df_prd_corr_wat["CORR_FAC"]=df_prd_corr_wat["CORR_FAC"].apply(lambda x:x if x>0 else 0)


# output_folder_temp = "C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1"

#Combine into a single file
df_prd_corr_final =pd.concat([df_prd_corr_liq, df_prd_corr_wat, df_prd_corr_wct])
df_prd_corr_final.to_csv(output_folder+"/"+userinput.prd_inj_corr_output, index=False)
# df_prd_corr_final.to_csv(output_folder_temp+"/"+userinput.prd_inj_corr_output, index=False)

df_prdinj_corr_inpdata.to_csv(output_folder+"/"+userinput.prd_inj_corr_inpdata, index=False)
# df_prdinj_corr_inpdata.to_csv(output_folder_temp+"/"+userinput.prd_inj_corr_inpdata, index=False)

print("*********************************************************************************")
print("Exported :"+userinput.prd_inj_corr_output)
print("Exported :"+userinput.prd_inj_corr_inpdata)
print("*********************************************************************************")

    

# %%

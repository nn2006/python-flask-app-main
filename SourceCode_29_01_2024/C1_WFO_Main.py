# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 01:33:45 2021

@author: Manu.Ujjwal
"""
"""Modified by: R.Agrawal

#check line#316 & 317 and make it more universal for any field that has horizontal wells


"""



import os
import sys
import fnmatch
import pandas as pd
import numpy as np
import geopandas as gpd
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

distmatrix_file = userinput.dist_matrix_output
corr_liq_file=userinput.prd_inj_corr_output
wmaster_file=userinput.wmaster_output
# press_file = "P1_BHP_Press.csv" #"01_BHP_Press.csv"
area_shpfile = "01_InjDrainage_DistMatrix_Shapefile.shp"
area_frac_shpfile = "01_IntersectPoly_well_pattern_Shapefile.shp"
pattern_file=userinput.pattern_map_output
pat_dist=userinput.pattern_dist_matrix
composite_id="COMPOSITE_ID"
wb_sn='WELLBORE_SHORT_NAME'

field_res1="FIELD_RES_GROUP"

pat_nm="PAT_NAME"



#%% Functions

def create_df_for_confactor_plot(sdf, con_factor_col_inj="CF_STATIC_I", con_factor_col_prd ="CF_STATIC_P", injname_col = "s_"+composite_id, prdname_col = composite_id):
    '''
    Objective: Create a file which can be used in Spotfire to display connectivity on map
    Arguements:
    sdf -> STIR output dataframe, preferebaly processed using renormalize_stir_confactor()
    injname_col -> composite id columns referring to injectors; default "s_"+composite_id
    prdname_col -> composite id columns referring to producers; default "COMPOSITE_ID"
    con_factor_col_PRD -> Prd Centric Connectivity factor column to transform, preferably post processed
    con_factor_col_inj -> Inj Centric Connectivity factor column to transform, preferably post processed
    '''
    sdf = sdf.rename(columns={injname_col:"Injector", prdname_col:"Producer"})
    sdf[con_factor_col_inj].fillna(0, inplace=True)
    sdf[con_factor_col_prd].fillna(0, inplace=True)

    sdf_pp_out = pd.DataFrame()
    for field_res, sdf_field_res in sdf.groupby(field_res1):
            
        sdf1 = sdf_field_res[["Injector","Producer", con_factor_col_inj,con_factor_col_prd,'CF_DYNA_I','CF_DYNA_P', "s_COMPL_MID_X", "s_COMPL_MID_Y",field_res1, pat_nm]].reset_index(drop=True)   
        sdf1["WELL_NAME"] = sdf1["Injector"]
        sdf1["WELL_TYPE"] = "Injector"
        sdf1 = sdf1.rename(columns={"s_COMPL_MID_X":"WELL_COMPL_MID_X", "s_COMPL_MID_Y":"WELL_COMPL_MID_Y"})
        sdf1.sort_values(["Injector","Producer"], inplace=True) 
                   
        sdf2 = sdf_field_res[["Injector","Producer", con_factor_col_inj,con_factor_col_prd,'CF_DYNA_I','CF_DYNA_P', "COMPL_MID_X", "COMPL_MID_Y",field_res1,pat_nm]].reset_index(drop=True)   
        sdf2["WELL_NAME"] = sdf2["Producer"]
        sdf2["WELL_TYPE"] = "Producer"
        sdf2 = sdf2.rename(columns={"COMPL_MID_X":"WELL_COMPL_MID_X", "COMPL_MID_Y":"WELL_COMPL_MID_Y"})
        sdf2.sort_values(["Producer","Injector"], inplace=True)            
        
        sdf_pp_out_tmp = pd.concat([sdf1, sdf2])   
        sdf_pp_out = pd.concat([sdf_pp_out, sdf_pp_out_tmp])              
        print("Completed: ", field_res)

    return sdf_pp_out      

def correlation_impact(corr_fact_df,inj_col,prod_col,corr_fact_col,connect_fac_col):
    """This function is used to calculate impact of injection rate with various production parameters and their correlation 
    in order to determine injector effciency and hence injector recommendation
    Arguments: 
    corr_fact_df --> dataframe with coorelation factors,
    inj_col --> injector ID column in corr_fact_df,
    prod_col --> producer ID column in corr_fact_df,
    prod_param_col --> producer paramter column in corr_fact_df,
    corr_fact_col --> correlation factor column in corr_fact_df,
    connect_fac_col --> connectivity factor column in corr_fact_df
    """
    
    df_corr_impact=pd.DataFrame(columns=[inj_col,"CORR_IMPACT"])
    for inj, df in corr_fact_df.groupby(inj_col):
        # print(inj)
        
        # for para, df_param in df.groupby(prod_param_col):
        df_param=df
        df_param1 =df_param[df_param[connect_fac_col]>0.01]
        producer_count = df_param1[prod_col].count()
        # print(producer_count)
        positive_corr_count = df_param1[df_param1[corr_fact_col]>0.0][corr_fact_col].count()
        # print(positive_corr_count)
        percent_positive_corr = (positive_corr_count/producer_count)
        df_corr_impact=df_corr_impact.append({inj_col:inj,"CORR_IMPACT":percent_positive_corr},ignore_index=True)
        # df_temp1[prod_param_col]=para
        # print(df_corr_impact)
    return df_corr_impact
            
#%% Add Calculated columns to distance matrix

#Import distance matrix and remove unrequired rows
df_wmaster_com=pd.read_csv(input_folder+"/"+wmaster_file)
df_pattern_file = pd.read_csv(input_folder+"/"+pattern_file)
df_wmaster_com=df_wmaster_com[df_wmaster_com["STIR_FLAG"]==1]

# df_pat_dist=pd.read_csv("C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1/"+pat_dist)


df_pat_dist=pd.read_csv(input_folder+"/"+pat_dist)

#%%
dist_df0 = pd.read_csv(input_folder+"/"+ distmatrix_file)
# dist_df0 = pd.read_csv("C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1/"+ distmatrix_file)

df_corr=pd.read_csv(input_folder+"/"+ corr_liq_file)
# df_bhpf=pd.read_csv(input_folder+"/"+press_file)
df_area = gpd.GeoDataFrame.from_file(input_folder+"/"+area_shpfile)
df_frac_area = gpd.GeoDataFrame.from_file(input_folder+"/"+area_frac_shpfile)
df_area=df_area.rename(columns={"s_WELLBORE":"s_"+wb_sn,
                                "s_COMPOSIT":"s_"+composite_id,"WELLBORE_S":wb_sn,
                                "COMPOSITE_":composite_id})

df_area[field_res1]=df_area["s_"+wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[field_res1])))
df_area = df_area[df_area["DrainageRa"]==userinput.area_drainage_radius]
df_frac_area1=pd.DataFrame()
for field in df_frac_area["FIELD_CODE"].unique():
    df_frac_temp = df_frac_area[df_frac_area["FIELD_CODE"]==field]
    df_frac_temp1=df_frac_temp[df_frac_temp["DR"]==userinput.frac_area_drainage_radius[field]]
    df_frac_area1=pd.concat([df_frac_area1,df_frac_temp1])

df_area["WP"]=df_area["s_"+wb_sn]+"_"+df_area[wb_sn]
# df_area[field_res1]= df_area["FIELD_CODE"]+"_"+df_area["RES_CODE"]
# df_area=df_area[df_area["FIELD_RES_GROUP"].isin(userinput.fldresgrp_hor_wells)]

df_corr["WELL_PAIR"]=df_corr["s_"+composite_id]+"/"+df_corr[composite_id]
df_corr_liq=df_corr[df_corr["PRD_PARAM"]=="C_LIQ_PD"]
df_corr_wat = df_corr[df_corr["PRD_PARAM"]=="C_WATER_PD"]
df_corr_wct = df_corr[df_corr["PRD_PARAM"]=="C_WCT_PD"]
dist_df = dist_df0[dist_df0["WELL_PAIR_TYPE"].isin(["WI_OP","OP-WI_OP"])].reset_index(drop=True)
dist_df = dist_df[dist_df["DISTANCE"]>0].reset_index(drop=True)



# dist_df["PAT_NAME"]=dist_df['WB_SHORTNAME'].map(dict(zip(df_pattern_file['WB_SHORTNAME'],df_pattern_file["PAT_NAME"])))
# dist_df["s_PAT_NAME"]=dist_df['s_WB_SHORTNAME'].map(dict(zip(df_pattern_file['WB_SHORTNAME'],df_pattern_file["PAT_NAME"])))

#%%Combining distance matrix and wmaster for rates

# df_wmaster_com=df_wmaster_com[df_wmaster_com["FIELD_NAME"].str.contains("|".join(userinput.field_list),regex=True,na=False)]
# df_wmaster_com=df_wmaster_com[df_wmaster_com["RES_CODE"].str.contains("|".join(userinput.reservoir_list),regex=True,na=False)]



df_prod_wmaster = df_wmaster_com[[composite_id,"COMPL_STATUS","ANA_START_DATE","ANA_END_DATE"]]
dict_pr_cumoil=dict(df_wmaster_com.groupby([composite_id])["C_MAX_CUMOIL"].max())
dict_pr_cumwater=dict(df_wmaster_com.groupby([composite_id])["C_MAX_CUMWATER"].max())


dict_wi_Last=dict(df_wmaster_com.groupby([composite_id])["C_INJ_PD_last_avg"].sum())
dict_wi_1wkavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_PD_1week_avg"].sum())
dict_wi_1monavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_PD_1mon_avg"].sum())
dict_wi_3monavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_PD_3mon_avg"].sum())
dict_wi_6monavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_PD_6mon_avg"].sum())
dict_wi_1yravg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_PD_1yr_avg"].sum())

dict_wi_cd_Last=dict(df_wmaster_com.groupby([composite_id])["C_INJ_CD_last_avg"].sum())
dict_wi_cd_1wkavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_CD_1week_avg"].sum())
dict_wi_cd_1monavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_CD_1mon_avg"].sum())
dict_wi_cd_3monavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_CD_3mon_avg"].sum())
dict_wi_cd_6monavg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_CD_6mon_avg"].sum())
dict_wi_cd_1yravg=dict(df_wmaster_com.groupby([composite_id])["C_INJ_CD_1yr_avg"].sum())

dict_wi_cum=dict(df_wmaster_com.groupby([composite_id])["C_MAX_CUMINJ"].max())

col_to_aggregate =["C_OIL_PD_last_avg","C_WATER_PD_last_avg","C_LIQ_PD_last_avg",
                                   "C_OIL_PD_1week_avg","C_WATER_PD_1week_avg","C_LIQ_PD_1week_avg", 
                                   "C_OIL_PD_1mon_avg","C_WATER_PD_1mon_avg","C_LIQ_PD_1mon_avg",
                                   "C_OIL_PD_3mon_avg","C_WATER_PD_3mon_avg","C_LIQ_PD_3mon_avg",
                                   "C_OIL_PD_6mon_avg","C_WATER_PD_6mon_avg","C_LIQ_PD_6mon_avg",
                                   "C_OIL_PD_1yr_avg","C_WATER_PD_1yr_avg","C_LIQ_PD_1yr_avg",
                                   "C_OIL_CD_last_avg","C_WATER_CD_last_avg","C_LIQ_CD_last_avg",
                                   "C_OIL_CD_1week_avg","C_WATER_CD_1week_avg","C_LIQ_CD_1week_avg", 
                                   "C_OIL_CD_1mon_avg","C_WATER_CD_1mon_avg","C_LIQ_CD_1mon_avg",
                                   "C_OIL_CD_3mon_avg","C_WATER_CD_3mon_avg","C_LIQ_CD_3mon_avg",
                                   "C_OIL_CD_6mon_avg","C_WATER_CD_6mon_avg","C_LIQ_CD_6mon_avg",
                                   "C_OIL_CD_1yr_avg","C_WATER_CD_1yr_avg","C_LIQ_CD_1yr_avg"]

def avg_rate_columns(df_master,column_to_aggregate,list):
    df_to_merge=df_master[[composite_id,"COMPL_STATUS"]]
    for col in list:
        dftemp11 = (df_master.groupby([column_to_aggregate])[col].sum())
        df_to_merge=pd.merge(df_to_merge,dftemp11,on=[composite_id],how="left")
    return df_to_merge
df_to_merge =avg_rate_columns(df_wmaster_com,composite_id,col_to_aggregate)

# dict_oil_3mon=dict(df_wmaster_com.groupby([composite_id])["C_OIL_PD_3mon_avg"].sum())
# dict_liq_3mon=dict(df_wmaster_com.groupby([composite_id])["C_LIQ_PD_3mon_avg"].sum())
# dict_wat_3mon=dict(df_wmaster_com.groupby([composite_id])["C_WATER_PD_3mon_avg"].sum())

# dict_oil_1yravg=dict(df_wmaster_com.groupby([composite_id])["C_OIL_PD_1yr_avg"].sum())
# dict_liq_1yravg=dict(df_wmaster_com.groupby([composite_id])["C_LIQ_PD_1yr_avg"].sum())
# dict_wat_1yravg=dict(df_wmaster_com.groupby([composite_id])["C_WATER_PD_1yr_avg"].sum())

dist_df=pd.merge(dist_df,df_prod_wmaster,on=[composite_id,"COMPL_STATUS"],how="left")
dist_df=pd.merge(dist_df,df_to_merge,on=[composite_id,"COMPL_STATUS"],how="left")

dist_df["C_INJ_PD_last_avg"]=dist_df["s_"+composite_id].map(dict_wi_Last)
dist_df["C_INJ_PD_1week_avg"]=dist_df["s_"+composite_id].map(dict_wi_1wkavg)
dist_df["C_INJ_PD_1mon_avg"]=dist_df["s_"+composite_id].map(dict_wi_1monavg)
dist_df["C_INJ_PD_3mon_avg"]=dist_df["s_"+composite_id].map(dict_wi_3monavg)
dist_df["C_INJ_PD_6mon_avg"]=dist_df["s_"+composite_id].map(dict_wi_6monavg)
dist_df["C_INJ_PD_1yr_avg"]=dist_df["s_"+composite_id].map(dict_wi_1yravg)

dist_df["C_INJ_CD_last_avg"]=dist_df["s_"+composite_id].map(dict_wi_cd_Last)
dist_df["C_INJ_CD_1week_avg"]=dist_df["s_"+composite_id].map(dict_wi_cd_1wkavg)
dist_df["C_INJ_CD_1mon_avg"]=dist_df["s_"+composite_id].map(dict_wi_cd_1monavg)
dist_df["C_INJ_CD_3mon_avg"]=dist_df["s_"+composite_id].map(dict_wi_cd_3monavg)
dist_df["C_INJ_CD_6mon_avg"]=dist_df["s_"+composite_id].map(dict_wi_cd_6monavg)
dist_df["C_INJ_CD_1yr_avg"]=dist_df["s_"+composite_id].map(dict_wi_cd_1yravg)


dist_df["INJ_CUM"]=dist_df["s_"+composite_id].map(dict_wi_cum)
dist_df["C_MAX_CUMOIL"]=dist_df[composite_id].map(dict_pr_cumoil)
dist_df["C_MAX_CUMWATER"]=dist_df[composite_id].map(dict_pr_cumwater)

# dist_df["C_OIL_PD_3mon_avg"]=dist_df[composite_id].map(dict_oil_3mon)
# dist_df["C_LIQ_PD_3mon_avg"]=dist_df[composite_id].map(dict_liq_3mon)
# dist_df["C_WATER_PD_3mon_avg"]=dist_df[composite_id].map(dict_wat_3mon)

# dist_df["C_OIL_PD_1yr_avg"]=dist_df[composite_id].map(dict_oil_1yravg)
# dist_df["C_WATER_PD_1yr_avg"]=dist_df[composite_id].map(dict_wat_1yravg)
# dist_df["C_LIQ_PD_1yr_avg"]=dist_df[composite_id].map(dict_liq_1yravg)
dist_df=dist_df.drop_duplicates()
#%%
#Calculate Zone match score, res code match score and liq correlation and BHP score
dist_df["ZONE_MATCH_SCORE"] = dist_df.apply(lambda x: fn.calc_zone_match_score(x["s_ZONE_OPEN"], x["ZONE_OPEN"], userinput.default_zonescore ), axis=1 )
dist_df["LIQ_CORR_FAC"]=dist_df["WELL_PAIR"].map(dict(zip(df_corr_liq["WELL_PAIR"],df_corr_liq["CORR_FAC"])))
dist_df["WAT_CORR_FAC"]=dist_df["WELL_PAIR"].map(dict(zip(df_corr_wat["WELL_PAIR"],df_corr_wat["CORR_FAC"])))
dist_df["WCT_CORR_FAC"]=dist_df["WELL_PAIR"].map(dict(zip(df_corr_wct["WELL_PAIR"],df_corr_wct["CORR_FAC"])))
# dist_df["BHP_FAC"]=dist_df[composite_id].map(dict(zip(df_bhpf[composite_id],df_bhpf["Press_Rank"])))
# dist_df["BHP"]=dist_df[composite_id].map(dict(zip(df_bhpf[composite_id],df_bhpf["BHP_Press"])))
# dist_df["BHP_Source"]=dist_df[composite_id].map(dict(zip(df_bhpf[composite_id],df_bhpf["Press_Source"])))
dist_df["WP"]=dist_df["s_"+wb_sn]+"_"+dist_df[wb_sn]
dist_df["OLP_AREA"]=dist_df["WP"].map(dict(zip(df_area["WP"],df_area["Area"])))
# dist_dft1=dist_df[dist_df["FIELD_CODE"].isin(["LEKHWAIR","KARIM WEST"])]
# dist_dft2=dist_df[~dist_df["FIELD_CODE"].isin(["LEKHWAIR","KARIM WEST"])]
dist_df["OLP_AREA"]=dist_df["OLP_AREA"].fillna(0)
# dist_df["PAT_NAME"]=dist_df["WP"].map(dict(zip(df_pat_dist["WELL_PAIR_SN"],df_pat_dist["PAT_NAME"])))

# dist_df["Well_PAT"]=dist_df[wb_sn]+"_"+dist_df[pat_nm]
# dist_df["s_Well_PAT"]=dist_df["s_"+wb_sn]+"_"+dist_df["s_"+pat_nm]
# df_frac_area1["WB_PAT"]=df_frac_area1["WB_SHORTNA"]+"_"+df_frac_area1["PAT_NAME"]
# dist_df["C_ALLOC_FAC_P"]=dist_df["Well_PAT"].map(dict(zip(df_frac_area1["WB_PAT"],df_frac_area1["INTERSEC_1"])))
# dist_df["C_ALLOC_FAC_I"]=dist_df["s_Well_PAT"].map(dict(zip(df_frac_area1["WB_PAT"],df_frac_area1["INTERSEC_1"])))

df_pat_temp1=df_pat_dist.drop_duplicates().groupby(["WELL_PAIR_SN"]).agg({"PAT_NAME":list})
df_pat_temp1=df_pat_temp1.reset_index()
dictpat_temp =dict(zip(df_pat_temp1["WELL_PAIR_SN"],df_pat_temp1["PAT_NAME"]))
dist_df["PAT_NAME"]=dist_df["WP"].map(dictpat_temp)
dist_df=dist_df.explode("PAT_NAME")

dist_df["CONN_OPEN_INJ"]=dist_df["WP"].map(dict(zip(df_pat_dist["WELL_PAIR_SN"],df_pat_dist["CONN_OPEN_INJ"])))

# dist_df.drop(columns=["Well_PAT"],inplace=True)
# dist_df.drop(columns=["s_Well_PAT"],inplace=True)

dict_pat_map = dict(zip(df_pattern_file[wb_sn],df_pattern_file[pat_nm])) 
dist_df_temp1=dist_df[~dist_df[pat_nm].isnull()]

dist_df_temp2=dist_df[dist_df[pat_nm].isnull()]
dist_df_temp3 =pd.DataFrame()
for wsn, dfwsn in dist_df_temp2.groupby([wb_sn]):
    if wsn in list(df_pattern_file[wb_sn].unique()):
        continue
    else:
        dfwsn[pat_nm] = dfwsn["s_"+wb_sn].map(dict_pat_map)
    dist_df_temp3=pd.concat([dist_df_temp3,dfwsn])

dist_df= pd.concat([dist_df_temp1,dist_df_temp3])

#%%
#Identify active well pair for analysis
dist_df["DIST_CUTOFF"] = dist_df[field_res1].map(userinput.dict_dist_cutoff)
dist_df["ZMS_CUTOFF"] = dist_df[field_res1].map(userinput.dict_zms_cutoff)

dist_df1=pd.DataFrame()
for pt1, dfpt1 in dist_df.groupby([pat_nm]):
    dfpt1["DIST_FLAG_1"]=1
    dist_df1=pd.concat([dist_df1,dfpt1])
dist_df1["DIST_FLAG_1"].fillna(0,inplace=True)
dist_df1["DIST_FLAG"]=dist_df1.apply(lambda x: x["DIST_FLAG_1"] if x["DISTANCE"]<= x["DIST_CUTOFF"] else 0 , axis=1 )
dist_df2=dist_df1[dist_df1['FIELD_CODE']=="SAIH RAWL"] #check and make it more universal for any field that has horizontal wells
dist_df3=dist_df1[dist_df1['FIELD_CODE']!="SAIH RAWL"]
dist_df2["DIST_FLAG"] =  dist_df2.apply(lambda x:x["DIST_FLAG_1"] if x["OLP_AREA"]>0 else 0 , axis=1 )
dist_df=pd.concat([dist_df2,dist_df3])

# dist_df["DIST_FLAG_1"] =  dist_df.apply(lambda x: 1 if x["DISTANCE"]<= x["DIST_CUTOFF"] else 0 , axis=1 )

# dist_df3=pd.DataFrame()
# #making dist_flag 1 for wells within same pattern
# for comp_id, dfcompid in dist_df.groupby([composite_id]):

#     dfcompid.reset_index(inplace=True, drop=True)
#     dfcompid1=dfcompid[~dfcompid["CONN_OPEN_INJ"].isnull()]
#     try:
#         dfcompid1["DIST_FLAG"]=dfcompid1.apply(lambda r: 1 if r["s_"+wb_sn] in r["CONN_OPEN_INJ"] else r["DIST_FLAG_1"],axis=1 )
#     except:
#         dfcompid1["DIST_FLAG"]=dfcompid1["DIST_FLAG_1"]
#     dfcompid2=dfcompid[dfcompid["CONN_OPEN_INJ"].isnull()]
#     dfcompid2["DIST_FLAG"]=dfcompid2["DIST_FLAG_1"]
#     dfcompid3=pd.concat([dfcompid1,dfcompid2])
#     dist_df3=dist_df3.append(dfcompid3)

#%%

dist_df["ZMS_FLAG"] =  dist_df.apply(lambda x: 1 if x["ZONE_MATCH_SCORE"]>= x["ZMS_CUTOFF"] else 0 , axis=1 )
dist_df["OPEN_FLAG"] =  dist_df.apply(lambda x: 1 if ((x["s_COMPL_STATUS"]=='OPEN') and (x["COMPL_STATUS"]=='OPEN')) else 0 , axis=1)
dist_df["ACTIVE_PAIR_FLAG"] =  dist_df.apply(lambda x: x["DIST_FLAG"]*x["ZMS_FLAG"]*x["OPEN_FLAG"] , axis=1 )
dist_df["ACTIVE_PAIR_FLAG_STATIC"]=dist_df.apply(lambda x: x["DIST_FLAG"]*x["ZMS_FLAG"], axis=1 )
# dist_df["ACTIVE_PAIR_FLAG"]=dist_df.apply(lambda x: 1 if x["ACTIVE_PAIR_FLAG_1"]==0 and x["OPEN_FLAG"]>0 and x["PAT_NAME"]==x["s_PAT_NAME"] else x["ACTIVE_PAIR_FLAG_1"], axis=1)
# dist_df1=dist_df[dist_df["FIELD_CODE"]!="LEKHWAIR"]
# dist_df2=dist_df[dist_df["FIELD_CODE"]=="LEKHWAIR"]

#%%
#Calculate scaler for producer centric connectivity factor, based on other nearby injectors in similar Azimuth
# azi_scaler_df =pd.DataFrame()  
# azi_scaler_df_details =pd.DataFrame()   
# azi_search_angle = 30
# for pwell, pdf0 in dist_df.groupby(composite_id):
#     pdf = pdf0[pdf0["DIST_FLAG"]==1].reset_index(drop=True)
#     if len(pdf)>0:
#         print("========================",pwell,"================================================")   
#         pwell_azi_out = fn.calc_azi_PCF_scaler(pdf, azi_search_angle)[0]
#         azi_scaler_df = azi_scaler_df.append(pwell_azi_out)
    
#         pwell_azi_details = fn.calc_azi_PCF_scaler(pdf, azi_search_angle)[1]
#         azi_scaler_df_details = azi_scaler_df_details.append(pwell_azi_details)
#     else: 
#         print(pwell, " : No injector within DistCutoff xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")          
# azi_scaler_df["WELL_PAIR"] = azi_scaler_df.apply(lambda x: x["s_COMPOSITE_ID"]+"/"+x["COMPOSITE_ID"], axis=1)      
# dist_df["PCF_SCALER_AZI"]  =   dist_df["WELL_PAIR"].map(dict(zip(azi_scaler_df["WELL_PAIR"],azi_scaler_df["PCF_SCALER_AZI"])))    
# dist_df["PCF_SCALER_AZI"].fillna(1, inplace=True)

# dist_df=pd.concat([dist_df1,dist_df2])
#%%
#collect attributes for calculation
dist_df1=dist_df[dist_df['FIELD_CODE']=="SAIH RAWL"] #check and make it more universal for any field that has horizontal wells
dist_df2=dist_df[dist_df['FIELD_CODE']!="SAIH RAWL"]
dist_df1["DMS"]=dist_df1.apply(lambda x: x["OLP_AREA"] if x["ACTIVE_PAIR_FLAG_STATIC"]==1 else 0, axis=1)
dist_df2["DMS"] = dist_df2.apply(lambda x: 1/x["DISTANCE"] if x["ACTIVE_PAIR_FLAG_STATIC"]==1 else 0, axis=1)
dist_df=pd.concat([dist_df2,dist_df1])
dist_df["ZMS"] = dist_df.apply(lambda x: x["ZONE_MATCH_SCORE"] if x["ACTIVE_PAIR_FLAG_STATIC"]==1 else 0, axis=1)
dist_df["LCFS"]=dist_df.apply(lambda x: x["LIQ_CORR_FAC"] if x["ACTIVE_PAIR_FLAG"]==1 else 0, axis=1)
# dist_df["BHPS"]=dist_df.apply(lambda x: x["BHP_FAC"] if x["ACTIVE_PAIR_FLAG"]==1 else 0, axis=1)
dist_df["LiqRate"]=dist_df.apply(lambda x: x["C_LIQ_PD_1yr_avg"] if x["ACTIVE_PAIR_FLAG"]==1 else 0, axis=1)
dist_df["InjRate"]=dist_df.apply(lambda x: x["C_INJ_PD_1yr_avg"] if x["ACTIVE_PAIR_FLAG"]==1 else 0, axis=1)
#%%
#Normalize collected attributes at each producer level
dist_df["n_DMS"] = dist_df['DMS'] / dist_df.groupby(composite_id)['DMS'].transform('sum') #distance score
dist_df["n_ZMS"] = dist_df['ZMS'] / dist_df.groupby(composite_id)['ZMS'].transform('sum') #zone match score
dist_df["n_LIQCORFAC"]=dist_df["LCFS"]/ dist_df.groupby(composite_id)['LCFS'].transform('sum')
# dist_df["n_BHPFAC"]=dist_df["BHPS"]/ dist_df.groupby(composite_id)['BHPS'].transform('sum')
df_liqrate_wp=pd.DataFrame()
for wp1, dfwp1 in dist_df.groupby("s_"+composite_id): 
    dfwp1["n_LiqRate"]=dfwp1["LiqRate"]/ dfwp1['LiqRate'].sum()
    df_liqrate_wp=pd.concat([df_liqrate_wp,dfwp1])
dist_df["n_LiqRate"] = dist_df["WELL_PAIR"].map(dict(zip(df_liqrate_wp["WELL_PAIR"],df_liqrate_wp["n_LiqRate"])))

#Normalize collected attributes at each injector level
dist_df["n_DMS_i"] = dist_df['DMS'] / dist_df.groupby("s_"+composite_id)['DMS'].transform('sum') #distance score
dist_df["n_ZMS_i"] = dist_df['ZMS'] / dist_df.groupby("s_"+composite_id)['ZMS'].transform('sum') #zone match score
dist_df["n_LIQCORFAC_i"]=dist_df["LCFS"]/ dist_df.groupby("s_"+composite_id)['LCFS'].transform('sum')
# dist_df["n_BHPFAC_i"]=dist_df["BHPS"]/ dist_df.groupby("s_"+composite_id)['BHPS'].transform('sum')
df_injrate_wp=pd.DataFrame()
for wp, dfwp in dist_df.groupby(composite_id): 
    dfwp["n_InjRate_i"]=dfwp["InjRate"]/ dfwp['InjRate'].sum()
    df_injrate_wp=pd.concat([df_injrate_wp,dfwp])
dist_df["n_InjRate_i"] = dist_df["WELL_PAIR"].map(dict(zip(df_injrate_wp["WELL_PAIR"],df_injrate_wp["n_InjRate_i"])))


#dist_df["n_PMS"] = dist_df['ZMS'] / dist_df.groupby(composite_id)['ZMS'].transform('sum') #pressure score
#dist_df["n_WMS"] = dist_df['ZMS'] / dist_df.groupby(composite_id)['ZMS'].transform('sum') #watercut score

dist_df["n_DMS"].fillna(0, inplace=True)
dist_df["n_ZMS"].fillna(0, inplace=True)
dist_df["n_LIQCORFAC"].fillna(0,inplace=True)
# dist_df["n_BHPFAC"].fillna(0,inplace=True)
dist_df["n_LiqRate"].fillna(0,inplace=True)

dist_df["n_DMS_i"].fillna(0, inplace=True)
dist_df["n_ZMS_i"].fillna(0, inplace=True)
dist_df["n_LIQCORFAC_i"].fillna(0,inplace=True)
# dist_df["n_BHPFAC_i"].fillna(0,inplace=True)
dist_df["n_InjRate_i"].fillna(0,inplace=True)
#%%
#define weightages for these attributes (0-1 range)
dist_df["wt_DMS"] = dist_df[field_res1].map(userinput.dict_dms_weightage)
dist_df["wt_ZMS"] = dist_df[field_res1].map(userinput.dict_zms_weightage)
dist_df["wt_LIQCORFAC"]=dist_df[field_res1].map(userinput.dict_liqcorr_weightage)
dist_df["wt_BHPFAC"]=dist_df[field_res1].map(userinput.dict_bhpress_weightage)
dist_df["wt_LiqRate"]=dist_df[field_res1].map(userinput.dict_liq_weightage)
dist_df["wt_InjRate"]=dist_df[field_res1].map(userinput.dict_inj_weightage)
dist_df=dist_df.drop_duplicates()

#%%
#PROD CENTRIC CONNECTIVITY FACTOR
#weightage score for connectivity & normalzed connectivty factor
dist_df["CF_SCORE_STATIC_P"] = dist_df.apply(lambda x: x["n_DMS"]*x["wt_DMS"] +  x["n_ZMS"]*x["wt_ZMS"], axis=1)
dist_df["CF_STATIC_P"] = dist_df["CF_SCORE_STATIC_P"] / dist_df.groupby(composite_id)["CF_SCORE_STATIC_P"].transform('sum')
dist_df["CF_STATIC_P"].fillna(0, inplace=True)


dist_df["CF_SCORE_DYNA_P"]=dist_df.apply(lambda x: x["n_DMS"]*x["wt_DMS"]*x["OPEN_FLAG"]+  x["n_ZMS"]*x["wt_ZMS"]*x["OPEN_FLAG"]+x["n_LIQCORFAC"]*x["wt_LIQCORFAC"]+x["n_InjRate_i"]*x["wt_InjRate"], axis=1)
# dist_df["CF_SCORE_DYNA_P"] = dist_df.apply(lambda x: x["CF_SCORE_DYNA_P"]*x["PCF_SCALER_AZI"], axis=1)
dist_df["CF_DYNA_P"] = dist_df["CF_SCORE_DYNA_P"] / dist_df.groupby(composite_id)["CF_SCORE_DYNA_P"].transform('sum')
dist_df["CF_DYNA_P"].fillna(0, inplace=True)

#%%
#INJ CENTRIC CONNECTIVITY FACTOR
#temporary assumption -need to updated after pressure data incorporation
dist_df["CF_SCORE_STATIC_I"] = dist_df.apply(lambda x: x["n_DMS"]*x["wt_DMS"] +  x["n_ZMS"]*x["wt_ZMS"], axis=1)
dist_df["CF_STATIC_I"] = dist_df["CF_SCORE_STATIC_I"] / dist_df.groupby('s_'+composite_id)["CF_SCORE_STATIC_I"].transform('sum')
dist_df["CF_STATIC_I"].fillna(0, inplace=True)

dist_df["CF_SCORE_DYNA_I"]=dist_df.apply(lambda x: x["n_DMS"]*x["wt_DMS"]*x["OPEN_FLAG"] +  x["n_ZMS"]*x["wt_ZMS"]*x["OPEN_FLAG"]+x["n_LIQCORFAC"]*x["wt_LIQCORFAC"]+x["n_LiqRate"]*x["wt_LiqRate"], axis=1)
dist_df["CF_DYNA_I"] = dist_df["CF_SCORE_DYNA_I"] / dist_df.groupby('s_'+composite_id)["CF_SCORE_DYNA_I"].transform('sum')
dist_df["CF_DYNA_I"].fillna(0, inplace=True)

wct_corr_impact = correlation_impact(dist_df,"s_"+composite_id,composite_id,corr_fact_col="WCT_CORR_FAC",connect_fac_col="CF_DYNA_I")
dist_df=pd.merge(dist_df,wct_corr_impact,on="s_"+composite_id,how="left")

# dist_df["Well_PAT"]=dist_df[wb_sn]+"_"+dist_df[pat_nm]
# dist_df["s_Well_PAT"]=dist_df["s_"+wb_sn]+"_"+dist_df[pat_nm]
# df_frac_area1["WB_PAT"]=df_frac_area1["WB_SHORTNA"]+"_"+df_frac_area1["PAT_NAME"]
# dist_df["C_ALLOC_FAC_P"]=dist_df["Well_PAT"].map(dict(zip(df_frac_area1["WB_PAT"],df_frac_area1["INTERSEC_1"])))
# dist_df["C_ALLOC_FAC_I"]=dist_df["s_Well_PAT"].map(dict(zip(df_frac_area1["WB_PAT"],df_frac_area1["INTERSEC_1"])))

# dist_df["C_ALLOC_FAC_P"]=dist_df["C_ALLOC_FAC_P"].apply(lambda x:1 if x>0.9 else x)
# dist_df["C_ALLOC_FAC_P"]=dist_df["C_ALLOC_FAC_P"].apply(lambda x:0 if x<0.1 else x)
# dist_df["C_ALLOC_FAC_I"]=dist_df["C_ALLOC_FAC_I"].apply(lambda x:1 if x>0.9 else x)
# dist_df["C_ALLOC_FAC_I"]=dist_df["C_ALLOC_FAC_I"].apply(lambda x:0 if x<0.1 else x)
#%% POST PROCESS INTO NEW DATAFRAME FOR SPOTFIRE MAP VISUALISATION

dist_df_short = dist_df[['s_'+composite_id,composite_id,'CF_STATIC_P','CF_STATIC_I',"CF_DYNA_P","CF_DYNA_I","s_COMPL_MID_X", "s_COMPL_MID_Y","COMPL_MID_X", "COMPL_MID_Y",'FIELD_RES_GROUP',pat_nm]]
dist_df_4plot = create_df_for_confactor_plot(dist_df_short)
dist_df_4plot=dist_df_4plot.drop_duplicates()


#%% Export Results

# output_folder_temp = "C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1"

dist_df.to_csv(output_folder+"/"+userinput.wfo_result_output, index=False)
dist_df_4plot.to_csv(output_folder+"/"+userinput.wfo_result_plot_output, index=False)

# dist_df.to_csv(output_folder_temp+"/"+userinput.wfo_result_output, index=False)
# dist_df_4plot.to_csv(output_folder_temp+"/"+userinput.wfo_result_plot_output, index=False)


print("--------------------------Exported Results-----------------------------------------------")

# azi_scaler_df.to_csv(output_folder+"/"+"P2_PCF_AZI_SCALER_SUMMARY.csv", index=False)
# azi_scaler_df_details.to_csv(output_folder+"/"+"P2_PCF_AZI_SCALER_DETAILS.csv", index=False)

# azi_scaler_df.to_csv(output_folder_temp+"/"+"P2_PCF_AZI_SCALER_SUMMARY.csv", index=False)
# azi_scaler_df_details.to_csv(output_folder_temp+"/"+"P2_PCF_AZI_SCALER_DETAILS.csv", index=False)


print("*********************************************************************************")
print("Exported:"+userinput.wfo_result_output )
print("Exported:"+userinput.wfo_result_plot_output)
print("*********************************************************************************")






# %%

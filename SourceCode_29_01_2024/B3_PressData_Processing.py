# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 11:15:47 2021

@author: R.Agrawal

File Description: This file contains py code for combining and pre-processing static
pressure data from different fields. 
    
Input description:
    Raw static pressure data files
    
Output description:
    1) File that contains static pressure data combined in one file from all fields
"""

#%%Importing libraries
import os
import sys
import fnmatch
import pandas as pd
import numpy as np
from scipy.spatial import distance_matrix
pd.set_option('display.max_columns', 1000)

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

wmaster_file=userinput.wmaster_output


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

wb_sn="WELLBORE_SHORT_NAME"
cnd_nm="CONDUIT_NAME"


#%% pressure data combining
df_wmaster_com=pd.read_csv(output_folder+"/"+wmaster_file)
df_wmaster_com=df_wmaster_com[df_wmaster_com[field_code].isin(userinput.field_list)]
df_wmaster_com=df_wmaster_com[df_wmaster_com["RES_CODE"].isin(userinput.reservoir_list)]
df_wmaster_com=df_wmaster_com[df_wmaster_com["STIR_FLAG"]==1]

# df_wmaster_com=df_wmaster_com[df_wmaster_com["FIELD_NAME"].str.contains("|".join(userinput.field_list),regex=True,na=False)]
# df_wmaster_com=df_wmaster_com[df_wmaster_com["RES_CODE"].str.contains("|".join(userinput.reservoir_list),regex=True,na=False)]



#%% combining static pressure files as received from RE of respective asset

#Static data pressure file for SR

df_static_pressure_combined=pd.DataFrame()
#proccessing Saih Rawl static pressure data file as received from asset team 
df_SR_stat_press = pd.read_excel(userinput.SR_pressure_data,sheet_name=userinput.SR_pressure_data_sheet,usecols="A:E",parse_dates=["Date2"])
df_SR_stat_press1=df_SR_stat_press[["Conduit","Date2","Pressure"]]
df_SR_stat_press1[wb_sn]=df_SR_stat_press1["Conduit"].map(dict(zip(df_wmaster_com[cnd_nm],df_wmaster_com[wb_sn])))
df_SR_stat_press1[composite_id]=df_SR_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[composite_id])))
df_SR_stat_press1.rename(columns={"Conduit":cnd_nm,"Date2":"Date","Pressure":"Datum_Pressure"},inplace=True)
df_SR_stat_press1[field_code]=df_SR_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[field_code])))
df_SR_stat_press1["RES_CODE"]=df_SR_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["RES_CODE"])))


#proccessing Karim West static pressure data file as received from asset team 
df_KW_stat_press = pd.read_excel(userinput.KW_pressure_data,sheet_name=userinput.KW_pressure_data_sheet,usecols="A:G",parse_dates=["date"],skiprows=1)
df_KW_stat_press1=df_KW_stat_press[df_KW_stat_press["RES"]=="HAIMA"]
df_KW_stat_press1=df_KW_stat_press1[["WELL_ID","date","Pressure, kpa"]]
df_KW_stat_press1[cnd_nm]=df_KW_stat_press1["WELL_ID"].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[cnd_nm])))
df_KW_stat_press1[composite_id]=df_KW_stat_press1["WELL_ID"].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[composite_id])))
df_KW_stat_press1.rename(columns={"WELL_ID":wb_sn,"date":"Date","Pressure, kpa":"Datum_Pressure"},inplace=True)
df_KW_stat_press1[field_code]=df_KW_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[field_code])))
df_KW_stat_press1["RES_CODE"]=df_KW_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["RES_CODE"])))


#proccessing Lekhwair static pressure data file as received from asset team 
df_LEK_stat_press = pd.read_excel(userinput.LEK_pressure_data,sheet_name=userinput.LEK_pressure_data_sheet,usecols="A:F",parse_dates=["DATE"],skiprows=1)
df_LEK_stat_press1=df_LEK_stat_press[df_LEK_stat_press["Field"]=="AN"]
df_LEK_stat_press1=df_LEK_stat_press1[["CONDUIT_NAME","DATE","Datum Pressure"]]
df_LEK_stat_press1[wb_sn]=df_LEK_stat_press1["CONDUIT_NAME"].map(dict(zip(df_wmaster_com[cnd_nm],df_wmaster_com[wb_sn])))
df_LEK_stat_press1[composite_id]=df_LEK_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[composite_id])))
df_LEK_stat_press1.rename(columns={"CONDUIT_NAME":cnd_nm,"DATE":"Date","Datum Pressure":"Datum_Pressure"},inplace=True)
df_LEK_stat_press1[field_code]=df_LEK_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com[field_code])))
df_LEK_stat_press1["RES_CODE"]=df_LEK_stat_press1[wb_sn].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["RES_CODE"])))
df_static_pressure_combined=pd.concat([df_static_pressure_combined,df_SR_stat_press1,df_KW_stat_press1,df_LEK_stat_press1])

df_static_pressure_combined1= df_static_pressure_combined[~df_static_pressure_combined[field_code].isna()]

#%%


#%%Writing processed files to output folder


df_static_pressure_combined1.to_csv(output_folder+"/P1_Static_Pressure_Data_Analytics.csv", index=False)

# %%

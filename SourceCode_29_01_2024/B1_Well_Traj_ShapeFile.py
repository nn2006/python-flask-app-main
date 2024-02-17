# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 13:59:02 2021

@author: Manu.Ujjwal
"""

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString
import sys
import fnmatch
import os
import glob
import chardet
pd.set_option('display.max_columns', 1000)

#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput

#%%%
dynamic_data_folder = userinput.dynamicdata_input_folder
manual_data_folder= userinput.manualdata_input_folder
static_data_folder=userinput.staticdata_input_folder
output_folder=userinput.output_folder
# dbtables_folder = userinput.dbtables_folder

file_wm = userinput.wmaster_output
file_dev = userinput.v_wb_dev
# file_perfhole = "0_PERF_PERFHOLES_COMBINED.csv"


#%%% defining column headers as variables

ab_name="Abbr_Name"
field_code="FIELD_CODE"
composite_id="COMPOSITE_ID"
w_type="WELL_TYPE"
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
geom="geometry"
wellbore="WELLBORE"

#%%Define required functions


def load_dataframe(dbfolder,filename):
    with open(os.path.join(dbfolder,filename), 'rb') as f:
        result = chardet.detect(f.read()) 
    df= pd.read_csv(os.path.join(dbfolder,filename),encoding=result['encoding'])
    return(df)

#%%
df_wm = pd.read_csv(output_folder+"/"+file_wm)
df_wm=df_wm.rename(columns={"EASTING":"XCOORD","NORTHING":"YCOORD"})

#In case mid comp X,Y is not available, use surface X,Y
df_wm.loc[df_wm[compl_mid_x].isna(), compl_mid_x] = df_wm.loc[df_wm[compl_mid_x].isna(), "XCOORD"]
df_wm.loc[df_wm[compl_mid_y].isna(), compl_mid_y] = df_wm.loc[df_wm[compl_mid_y].isna(), "YCOORD"]

#create point for Surface X,Y
df_well_xy_surface = df_wm[[composite_id,"XCOORD","YCOORD"]]
df_well_xy_surface[geom] = df_well_xy_surface.apply(lambda x: Point(float(x.XCOORD), float(x.YCOORD)), axis=1)
df_well_xy_surface['DATA_TYPE'] = "SURFACE_XY"

#create point for Mid Comp X,Y
df_well_xy_midcomp = df_wm[[composite_id,compl_mid_x,compl_mid_y]]
df_well_xy_midcomp[geom] = df_well_xy_midcomp.apply(lambda x: Point(float(x.COMPL_MID_X), float(x.COMPL_MID_Y)), axis=1)
df_well_xy_midcomp['DATA_TYPE'] = "MID_COMPL_XY"

#Combine the two files
df_well_xy = pd.concat([df_well_xy_surface, df_well_xy_midcomp],sort=True)
dict_field=dict(zip(df_wm["COMPOSITE_ID"],df_wm["FIELD_CODE"]))
dict_res_grp=dict(zip(df_wm["COMPOSITE_ID"],df_wm["RES_CODE"]))
df_well_xy["FIELD_CODE"]=df_well_xy["COMPOSITE_ID"].map(dict_field)
df_well_xy["RES_CODE"]=df_well_xy["COMPOSITE_ID"].map(dict_res_grp)
gdf_well_xy = gpd.GeoDataFrame(df_well_xy, geometry=geom)

#Export results
gdf_well_xy.to_file(output_folder+"/01_Well_XY_ShapeFile.shp", driver='ESRI Shapefile')

print("*****************************************************************************************")
print("Exported: 01_Well_XY_ShapeFile.shp")
print("*****************************************************************************************")

#%% CREATE A GEOPANDAS DATAFRAME WHERE TRAJECTORY IS STORED AS LINESTRING
#=================================================================================================
welluwi="WELL_UWI"
df_dev0 = load_dataframe(userinput.dbtables_folder1,file_dev)
# df_dev0 = df_dev0.rename(columns={"well":wellbore})
# df_dev0["TVDSS"] = df_dev0["TVD"]

#df_dev = df_dev0[['UWI', 'MD',  'TVD', 'KB', 'TVDSS','PROJECTED_X', 'PROJECTED_Y']]
#df_dev.sort_values(["UWI","MD"], inplace=True)

def generate_wellpath_shapefile(wdf, wellname_col, x_col, y_col):    
    #zip the coordinates into a point object and convert to a GeoData Frame
    geometry = [Point(xy) for xy in zip(wdf[x_col], wdf[y_col])]
    #create a geopandas data frame where X,Y is stored as points
    geo_df = gpd.GeoDataFrame(wdf, geometry=geometry)
    #create another geopandas data frame where points are converted to linestring
    geo_df2 = geo_df.groupby([wellname_col])[geom].apply(lambda x: LineString(x.tolist()))
    geo_df2 = gpd.GeoDataFrame(geo_df2, geometry=geom)       
    geo_df2.reset_index(inplace=True)
    return geo_df2
df_dev0["XCOORD"]= df_dev0[welluwi].map(dict(zip(df_wm[welluwi], df_wm["XCOORD"])))
df_dev0["YCOORD"]=df_dev0[welluwi].map(dict(zip(df_wm[welluwi], df_wm["YCOORD"])))
df_dev0['PROD_INTV_MID']=df_dev0[welluwi].map(dict(zip(df_wm[welluwi], df_wm['MID_PROD_INTERVAL_DFE'])))

df_dev0 =df_dev0.rename(columns={"DEFLECTION_EW":"XDELT","DEFLECTION_NS":"YDELT"})
df_dev0["X"]=df_dev0["XCOORD"]+df_dev0["XDELT"]
df_dev0["Y"]=df_dev0["YCOORD"]+df_dev0["YDELT"]

dict_field=dict(zip(df_wm[welluwi],df_wm["FIELD_CODE"]))
dict_res_grp=dict(zip(df_wm[welluwi],df_wm["RES_CODE"]))
dict_comid=dict(zip(df_wm[welluwi],df_wm["COMPOSITE_ID"]))
df_dev0["FIELD_CODE"]=df_dev0[welluwi].map(dict_field)
df_dev0["RES_CODE"]=df_dev0[welluwi].map(dict_res_grp)
df_dev0["COMPOSITE_ID"]=df_dev0[welluwi].map(dict_comid)
df_dev0.dropna(subset=["COMPOSITE_ID"],inplace=True)

df_dev0=df_dev0[df_dev0["FIELD_CODE"].isin(userinput.field_list)]


df_traj_shp = pd.DataFrame()
for well in df_dev0[welluwi].unique():   
    wdf_dev = df_dev0[df_dev0[welluwi]==well].reset_index(drop=True)
    wdf_dev.sort_values([welluwi,"AHD_DFE"], inplace=True)
    wdf_dev_shp = generate_wellpath_shapefile(wdf_dev,welluwi,"X","Y")
    df_traj_shp = pd.concat([df_traj_shp , wdf_dev_shp])
    print("Completed : ", well)
# dict_field=dict(zip(df_wm[wellbore],df_wm["FIELD_CODE"]))
# dict_res_grp=dict(zip(df_wm[wellbore],df_wm["RES_CODE"]))
# dict_comid=dict(zip(df_wm[wellbore],df_wm["COMPOSITE_ID"]))
df_traj_shp["FIELD_CODE"]=df_traj_shp[welluwi].map(dict_field)
df_traj_shp["RES_CODE"]=df_traj_shp[welluwi].map(dict_res_grp)
df_traj_shp["COMPOSITE_ID"]=df_traj_shp[welluwi].map(dict_comid)
df_traj_shp.dropna(subset=["COMPOSITE_ID"],inplace=True)
df_traj_shp  = gpd.GeoDataFrame(df_traj_shp , geometry=geom) 

df_traj_shp.to_crs = {'init' :'epsg:4326'}
df_traj_shp.to_file(output_folder +"/01_Traj_Shapefile.shp", driver='ESRI Shapefile')   

print("*****************************************************************************************")
print("Exported: 01_Traj_Shapefile.shp")
print("*****************************************************************************************")


#%% CREATE A GEOPANDAS DATAFRAME WHERE PERF HOLE SECTION IS STORED AS LINESTRING
#=================================================================================================
df_ph = df_wm.copy()

df_perf_shp = pd.DataFrame()
for well in df_dev0[welluwi].unique(): 
    print(well)
    wdf_dev = df_dev0[df_dev0[welluwi]==well].reset_index(drop=True)
    wdf_dev.sort_values([welluwi,"AHD_DFE"], inplace=True)
    # print(wdf_dev)
    wdf_ph = df_ph.loc[df_ph[welluwi]==well].reset_index(drop=True)
    # print(wdf_ph)
    for i in range(0, len(wdf_ph)):
        intv_top = wdf_ph.loc[i,['MIN_PROD_INTERVAL_DFE']].values[0]
        intv_bot = wdf_ph.loc[i,['MAX_PROD_INTERVAL_DFE']].values[0]
        # intv_status = wdf_ph.loc[i,['Prod Intv Status']].values[0]
        # print("top",intv_top)
        # print("bottom",intv_bot)
        # print(wdf_dev)
        wdf_dev_intv = wdf_dev.loc[wdf_dev["AHD_DFE"]>=intv_top]
        wdf_dev_intv = wdf_dev_intv.loc[wdf_dev["AHD_DFE"]<=intv_bot]
        # print(wdf_dev_intv)
        # print(len(wdf_dev_intv))
        if len(wdf_dev_intv)>1:
            wdf_dev_intv_shp = generate_wellpath_shapefile(wdf_dev_intv,welluwi,"X","Y")
            wdf_dev_intv_shp['IntvTop'] = intv_top
            wdf_dev_intv_shp['IntvBase'] = intv_bot
            # wdf_dev_intv_shp['IntvStatus'] = intv_status            
            df_perf_shp = pd.concat([df_perf_shp , wdf_dev_intv_shp])
            print("Completed : ", well)
        else:
            print(well ," : No traj section for this interval")

df_perf_shp["FIELD_CODE"] = df_perf_shp[welluwi].map(dict(zip(df_wm[welluwi],df_wm["FIELD_CODE"])))
df_perf_shp["COMPL_STATUS"] = df_perf_shp[welluwi].map(dict(zip(df_wm[welluwi],df_wm["COMPL_STATUS"])))
df_perf_shp["WELLBORE_NAME"] = df_perf_shp[welluwi].map(dict(zip(df_wm[welluwi],df_wm["WELLBORE_NAME"])))
df_perf_shp["WELLBORE_SHORT_NAME"] = df_perf_shp[welluwi].map(dict(zip(df_wm[welluwi],df_wm["WELLBORE_SHORT_NAME"])))



#%%
df_perf_shp  = gpd.GeoDataFrame(df_perf_shp , geometry='geometry') 
df_perf_shp.to_crs = {'init' :'epsg:4326'}
df_perf_shp.to_file(output_folder +"/01_PerfTraj_Shapefile.shp", driver='ESRI Shapefile')   

print("*****************************************************************************************")
print("Exported: 01_PerfHole_Shapefile.shp")
print("*****************************************************************************************")


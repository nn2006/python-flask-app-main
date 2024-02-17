# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 13:56:12 2022

@author: Manu.Ujjwal
"""
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString
import matplotlib.pylab as pylab
import matplotlib.pyplot as plt

import os
import glob
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_columns', 1000)



code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)
import userinput


output_folder=userinput.output_folder
input_folder = userinput.output_folder

field_list=userinput.field_list



#input_folder = 'C:/Users/Manu.Ujjwal/OneDrive - Shell/00_MY_DOCS/00_SAA/PDO/2021'
#output_folder = 'C:/Users/Manu.Ujjwal/OneDrive - Shell/00_MY_DOCS/00_SAA/PDO/2021'
#
#%% Read Input files
dist_matrix_file ='P1_DISTANCE_MATRIX_ANALYTICS.csv'
dist_df = pd.read_csv(input_folder+"/"+dist_matrix_file)
dist_df = dist_df[dist_df.FIELD_CODE.isin(field_list)]

df_perf_shp = gpd.GeoDataFrame.from_file(output_folder +"/01_PerfTraj_Shapefile.shp")

df_perf_shp=df_perf_shp.rename(columns={"WELLBORE_N":"WELLBORE_NAME","WELLBORE_S":"WELLBORE_SHORT_NAME"})
df_perf_shp=df_perf_shp.drop_duplicates()
file_wm = 'P0_MASTERXY_ANALYTICS.csv'
df_wm0 = pd.read_csv(input_folder+"/"+file_wm)
df_wm = df_wm0[df_wm0.FIELD_CODE.isin(field_list)]


wb_sn= "WELLBORE_SHORT_NAME"#"WB_SHORTNAME"
wb_nm="WELLBORE_NAME"
#%%
dist_df = dist_df[dist_df.WELL_PAIR_TYPE =="WI_OP"]
df_wm = df_wm.rename(columns={"EASTING":"X","NORTHING":"Y"})
df_wm=df_wm[~df_wm["X"].isnull()]
#In case mid comp X,Y is not available, use surface X,Y
df_wm.loc[df_wm["COMPL_MID_X"].isna(), "COMPL_MID_X"] = df_wm.loc[df_wm["COMPL_MID_X"].isna(), "X"]
df_wm.loc[df_wm["COMPL_MID_Y"].isna(), "COMPL_MID_Y"] = df_wm.loc[df_wm["COMPL_MID_Y"].isna(), "Y"]

#create point for Surface X,Y
df_well_xy_surface = df_wm.loc[:,["COMPOSITE_ID","X","Y"]]
df_well_xy_surface['geometry'] = df_well_xy_surface.apply(lambda x: Point(float(x.X), float(x.Y)), axis=1)
df_well_xy_surface['DATA_TYPE'] = "SURFACE_XY"

#create point for Mid Comp X,Y
df_well_xy_midcomp = df_wm.loc[:,["COMPOSITE_ID","COMPL_MID_X","COMPL_MID_Y"]]
df_well_xy_midcomp['geometry'] = df_well_xy_midcomp.apply(lambda x: Point(float(x.COMPL_MID_X), float(x.COMPL_MID_Y)), axis=1)
df_well_xy_midcomp['DATA_TYPE'] = "MID_COMPL_XY"
df_well_xy_midcomp1 = df_well_xy_midcomp.copy()
df_well_xy_midcomp1[wb_sn] = df_well_xy_midcomp1["COMPOSITE_ID"].map(dict(zip(df_wm["COMPOSITE_ID"], df_wm[wb_sn])))
df_well_xy_midcomp1[wb_nm] = df_well_xy_midcomp1["COMPOSITE_ID"].map(dict(zip(df_wm["COMPOSITE_ID"], df_wm[wb_nm])))

df_well_xy_midcomp1["geometry"] = df_well_xy_midcomp1[wb_nm].map(dict(zip(df_perf_shp[wb_nm], df_perf_shp["geometry"])))

df_well_xy_midcomp1["geometry_midxy"] =df_well_xy_midcomp1.apply(lambda x: LineString(
        [Point(float(x.COMPL_MID_X), float(x.COMPL_MID_Y)),Point(float(x.COMPL_MID_X)+1.0, float(x.COMPL_MID_Y)+1.0)]), axis=1)
df_well_xy_midcomp1.loc[df_well_xy_midcomp1["geometry"].isna(), "geometry"] = df_well_xy_midcomp1.loc[df_well_xy_midcomp1["geometry"].isna(), "geometry_midxy"]

df_well_xy_midcomp1 = gpd.GeoDataFrame(df_well_xy_midcomp1 , geometry='geometry') 
df_well_xy_midcomp1["FIELD_CODE"] = df_well_xy_midcomp1["COMPOSITE_ID"].map(dict(zip(df_wm["COMPOSITE_ID"], df_wm["FIELD_CODE"])))
df_well_xy_midcomp1["DrainLength"] = list(df_well_xy_midcomp1["geometry"].length)



#%%

inj_drainage_df = pd.DataFrame()
dist_rng = [50,100,150]

df_drng_polygon = gpd.GeoDataFrame()

for dr in dist_rng:
    df_well_xy_midcomp1["geometry_drn_poly"] = df_well_xy_midcomp1["geometry"].buffer(dr)
    dict_drngbox = dict(zip(df_well_xy_midcomp1[wb_sn] , df_well_xy_midcomp1["geometry_drn_poly"]))
    
    tmpdf_drng_poly = gpd.GeoDataFrame()
    tmpdf_drng_poly = df_well_xy_midcomp1[[wb_sn,"COMPOSITE_ID",'geometry_drn_poly','DrainLength']]
    tmpdf_drng_poly["DrainageRadius"] = dr
    df_drng_polygon = df_drng_polygon.append(tmpdf_drng_poly) 

    for iwell, idf in dist_df.groupby("s_"+wb_sn):
        #idf = dist_df[dist_df["s_"+wb_sn]=='SR-304H2']
        idf_inj = idf.copy().reset_index(drop=True)
        idf_inj["geometry"] = idf_inj["s_"+wb_sn].map(dict_drngbox)
        #idf_inj.dropna(subset=['geometry'], inplace=True)
        idf_inj = idf_inj[["s_"+wb_sn,'geometry','FIELD_CODE']]
        idf_inj = gpd.GeoDataFrame(idf_inj , geometry='geometry') 
       
        idf_prd = idf.copy().reset_index(drop=True)
        idf_prd["geometry"] = idf_prd[wb_sn].map(dict_drngbox)
        #idf_prd.dropna(subset=['geometry'], inplace=True)  
        idf_prd = idf_prd[[wb_sn,'geometry','FIELD_CODE']]
        idf_prd = gpd.GeoDataFrame(idf_prd, geometry='geometry') 
        
        intersect_poly_list=[]
        union_poly_list=[]
        intersect_area_list=[]
        for i in range(0, len(idf)):
            inj_poly = idf_inj["geometry"].tolist()[i]
            prd_poly = idf_prd["geometry"].tolist()[i]
            intersect_poly = inj_poly.intersection(prd_poly)
            union_poly = inj_poly.union(prd_poly)
            intersect_area = intersect_poly.area
            intersect_poly_list.append(intersect_poly)
            union_poly_list.append(union_poly)
            intersect_area_list.append(intersect_area)
               
        idf["InjDrainPoly"] = idf_inj["geometry"].tolist()
        idf["PrdDrainPoly"] = idf_prd["geometry"].tolist()
        idf["IntersectPoly"] = intersect_poly_list
        idf["geometry"] = union_poly_list
        idf["Area"] = intersect_area_list
        idf["DrainageRadius"] = dr
        
        idf1 = idf[[ 'FIELD_CODE','RES_CODE',"s_"+wb_sn, 's_COMPOSITE_ID',wb_sn,'COMPOSITE_ID','DISTANCE', 'WELL_PAIR', "Area","DrainageRadius","IntersectPoly","geometry"]]
        idf1 = idf1[idf1["Area"]>0].reset_index(drop=True)
        inj_drainage_df = inj_drainage_df.append(idf1)
        print(iwell,": Radius :", dr," : intersection calculated")

inj_drainage_df_shp = inj_drainage_df.copy()
del inj_drainage_df_shp["IntersectPoly"]

inj_drainage_df_shp["s_DrainLength"] = inj_drainage_df_shp["s_COMPOSITE_ID"].map(dict(zip(df_well_xy_midcomp1["COMPOSITE_ID"], df_well_xy_midcomp1["DrainLength"])))
inj_drainage_df_shp["DrainLength"] = inj_drainage_df_shp["COMPOSITE_ID"].map(dict(zip(df_well_xy_midcomp1["COMPOSITE_ID"], df_well_xy_midcomp1["DrainLength"])))




intersect_poly_df_shp = inj_drainage_df.copy()
intersect_poly_df_shp["geometry"] = intersect_poly_df_shp["IntersectPoly"]
del intersect_poly_df_shp["IntersectPoly"]
intersect_poly_df_shp = intersect_poly_df_shp[[ 'FIELD_CODE','s_COMPOSITE_ID','COMPOSITE_ID','WELL_PAIR', "Area","DrainageRadius","geometry"]]


#%%

# output_folder_temp = "C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1"

inj_drainage_df_shp = gpd.GeoDataFrame(inj_drainage_df_shp , geometry='geometry')  
inj_drainage_df_shp.to_crs = {'init' :'epsg:4326'}
inj_drainage_df_shp.to_file(output_folder +"/01_InjDrainage_DistMatrix_Shapefile.shp", driver='ESRI Shapefile')   
# inj_drainage_df_shp.to_file(output_folder_temp +"/01_InjDrainage_DistMatrix_Shapefile.shp", driver='ESRI Shapefile')   

intersect_poly_df_shp = gpd.GeoDataFrame(intersect_poly_df_shp , geometry='geometry')  
intersect_poly_df_shp.to_crs = {'init' :'epsg:4326'}
intersect_poly_df_shp.to_file(output_folder +"/01_IntersectPoly_DistMatrix_Shapefile.shp", driver='ESRI Shapefile')   
# intersect_poly_df_shp.to_file(output_folder_temp +"/01_IntersectPoly_DistMatrix_Shapefile.shp", driver='ESRI Shapefile')   

df_drng_polygon["FIELD_CODE"]=df_drng_polygon[wb_sn].map(dict(zip(df_wm[wb_sn],df_wm["FIELD_CODE"])))
df_drng_polygon["RES_CODE"]=df_drng_polygon[wb_sn].map(dict(zip(df_wm[wb_sn],df_wm["RES_CODE"])))

df_drng_polygon_shp = gpd.GeoDataFrame(df_drng_polygon , geometry='geometry_drn_poly') 
df_drng_polygon_shp.to_file(output_folder +"/01_DrainageAreaPoly_Shapefile.shp", driver='ESRI Shapefile')   
# df_drng_polygon_shp.to_file(output_folder_temp +"/01_DrainageAreaPoly_Shapefile.shp", driver='ESRI Shapefile')   




# %%

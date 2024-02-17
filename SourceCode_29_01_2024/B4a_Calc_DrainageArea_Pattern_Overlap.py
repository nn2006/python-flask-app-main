# -*- coding: utf-8 -*-
"""
Created on Sun Apr 10 02:28:23 2022

@author: Manu.Ujjwal
"""

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString
import matplotlib.pylab as pylab
import matplotlib.pyplot as plt
import tqdm
import os
import glob
import chardet
import warnings
warnings.filterwarnings("ignore")


pd.set_option('display.max_columns', 1000)


#%%
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)
import userinput


output_folder=userinput.output_folder
input_folder = userinput.output_folder
# dbtables_folder = userinput.dbtables_folder

field_list=userinput.field_list

wellb="WELLBORE"
geom="geometry"
wb_sn="WELLBORE_SHORT_NAME"
composite_id="COMPOSITE_ID"
w_nm="WELL_NAME"
w_snm="WELL_SHORT_NAME"
wb_nm="WELLBORE_NAME"
#%% defining required functions

def load_dataframe(dbfolder,filename):
    with open(os.path.join(dbfolder,filename), 'rb') as f:
        result = chardet.detect(f.read()) 
    df= pd.read_csv(os.path.join(dbfolder,filename),encoding=result['encoding'])
    return(df)


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

#
#%% Read Input files

# df_perf_shp = gpd.GeoDataFrame.from_file(output_folder +"/01_PerfTraj_Shapefile.shp")
df_pat_shp = gpd.GeoDataFrame.from_file(output_folder +"/Pat_combined.shp")
df_deviation=load_dataframe(userinput.dbtables_folder1,userinput.v_wb_dev)

file_wm = 'P0_MASTERXY_ANALYTICS.csv'
df_wm0 = pd.read_csv(input_folder+"/"+file_wm)
df_wm = df_wm0[df_wm0.FIELD_CODE.isin(field_list)]

#%%
df_wm = df_wm.rename(columns={"EASTING":"X","NORTHING":"Y"})

#In case mid comp X,Y is not available, use surface X,Y
df_wm.loc[df_wm["COMPL_MID_X"].isna(), "COMPL_MID_X"] = df_wm.loc[df_wm["COMPL_MID_X"].isna(), "X"]
df_wm.loc[df_wm["COMPL_MID_Y"].isna(), "COMPL_MID_Y"] = df_wm.loc[df_wm["COMPL_MID_Y"].isna(), "Y"]

#%%adding perf start MD and perf end MD for each drain based on perf start MD and perf end MD of latest drain 
# df_wmtemp =df_wm[df_wm[w_nm]=="SAIH RAWL-169"]
df_wm1 =pd.DataFrame()

tq = tqdm.tqdm(df_wm.groupby([w_nm]))
for wellbore, df_wb in tq:
    df_wb["PERF_TOP_MD"]=df_wb["MIN_PROD_INTERVAL_DFE"].max()
    df_wb["PERF_BTM_MD_temp"]=df_wb["MAX_PROD_INTERVAL_DFE"].max()
    wd=df_wb.loc[df_wb["STIR_FLAG"]==1,wb_sn].reset_index(drop=True)
       
    # df_wm_temp1=df_wm_temp.drop_duplicates().groupby([wb_sn]).agg({composite_id:list})
    try:
        for w in list(wd):
            if w == df_wb[wb_sn]: 
                df_wb["WELLID"]=w
                df_wm1=pd.concat([df_wm1,df_wb])
            else:continue
    except:
        try:
            df_wb["WELLID"]=df_wb[wb_sn].unique()
            df_wm1=pd.concat([df_wm1,df_wb])
        except:
            df_wb["WELLID"]=df_wb[wb_sn]
            df_wm1=pd.concat([df_wm1,df_wb])
    # df_wb1=df_wb.explode("WELLID")
    
df_wm1["PERF_BTM_MD"]=df_wm1.apply(lambda x: x["TD_DFE"] if x["PERF_BTM_MD_temp"]<x["TD_DFE"] else x["PERF_BTM_MD_temp"],axis=1)

df_wm1.drop(columns=["PERF_BTM_MD_temp"],inplace=True)
df_wm=df_wm1.copy()

dict_perf_top=dict(zip(df_wm[wb_nm],df_wm["PERF_TOP_MD"]))
dict_perf_btm=dict(zip(df_wm[wb_nm],df_wm["PERF_BTM_MD"]))
dict_surface_x=dict(zip(df_wm[wb_nm],df_wm["X"]))
dict_surface_y=dict(zip(df_wm[wb_nm],df_wm["Y"]))

#%%Adding perf top and perf bottom md in deviation file
# hl_nm="HOLE_NAME"
df_deviation=df_deviation.rename(columns={"AHD_DFE":"MD","HOLE_NAME":wb_nm,"DEFLECTION_EW":"XDELT","DEFLECTION_NS":"YDELT"})
df_deviation["PERF_TOP_MD"]=df_deviation[wb_nm].map(dict_perf_top)
df_deviation["PERF_BTM_MD"]=df_deviation[wb_nm].map(dict_perf_btm)
df_deviation["X"]=df_deviation[wb_nm].map(dict_surface_x)
df_deviation["Y"]=df_deviation[wb_nm].map(dict_surface_y)

df_deviation["X_SS"]=df_deviation["X"]+df_deviation["XDELT"]
df_deviation["Y_SS"]=df_deviation["Y"]+df_deviation["YDELT"]

df_drain_dev=pd.DataFrame()
tq1=tqdm.tqdm(df_deviation.groupby([wb_nm]))
for wb,df_wb in tq1:
    perf_top = df_wb["PERF_TOP_MD"].max()
    perf_btm= df_wb["PERF_BTM_MD"].max()
    df_wb1 = df_wb[(df_wb["MD"]>perf_top) & (df_wb["MD"]<perf_btm)]
    df_drain_dev=pd.concat([df_drain_dev,df_wb1])

#%% generating well drains and drain shapefile from deviation file

df_dev0=df_drain_dev.copy()
df_drain_shp = pd.DataFrame()
tq2= tqdm.tqdm(df_dev0[wb_nm].unique())
for well in tq2:   
    wdf_dev = df_dev0[df_dev0[wb_nm]==well].reset_index(drop=True)
    wdf_dev.sort_values([wb_nm,"MD"], inplace=True)
    try:
        wdf_dev_shp = generate_wellpath_shapefile(wdf_dev,wb_nm,"X_SS","Y_SS")
    except:
        wdf_dev=df_deviation[df_deviation[wb_nm]==well].reset_index(drop=True)
        wdf_dev.sort_values([wb_nm,"MD"], inplace=True)
        wdf_dev_shp = generate_wellpath_shapefile(wdf_dev,wb_nm,"X_SS","Y_SS")
    df_drain_shp = pd.concat([df_drain_shp , wdf_dev_shp])
    print("Completed : ", well)
dict_field=dict(zip(df_wm[wb_nm],df_wm["FIELD_CODE"]))
# dict_res_grp=dict(zip(df_wm[wb],df_wm["RES_GROUP"]))
dict_wbsn=dict(zip(df_wm[wb_nm],df_wm[wb_sn]))
df_drain_shp["FIELD_CODE"]=df_drain_shp[wb_nm].map(dict_field)
df_drain_shp[wb_sn]=df_drain_shp[wb_nm].map(dict_wbsn)

#%%
#create point for Surface X,Y
df_well_xy_surface = df_wm.loc[:,[wb_nm,composite_id,"X","Y"]]
df_well_xy_surface['geometry'] = df_well_xy_surface.apply(lambda x: Point(float(x.X), float(x.Y)), axis=1)
df_well_xy_surface['DATA_TYPE'] = "SURFACE_XY"

#create point for Mid Comp X,Y
df_well_xy_midcomp = df_wm.loc[:,[wb_nm,composite_id,"COMPL_MID_X","COMPL_MID_Y"]]
df_well_xy_midcomp['geometry'] = df_well_xy_midcomp.apply(lambda x: Point(float(x.COMPL_MID_X), float(x.COMPL_MID_Y)), axis=1)
df_well_xy_midcomp['DATA_TYPE'] = "MID_COMPL_XY"
df_well_xy_midcomp1 = df_well_xy_midcomp.copy()
df_well_xy_midcomp1[wb_sn] = df_well_xy_midcomp1[composite_id].map(dict(zip(df_wm[composite_id], df_wm[wb_sn])))
df_well_xy_midcomp1["geometry"] = df_well_xy_midcomp1[wb_sn].map(dict(zip(df_drain_shp[wb_sn], df_drain_shp["geometry"])))

df_well_xy_midcomp1["geometry_midxy"] =df_well_xy_midcomp1.apply(lambda x: LineString(
        [Point(float(x.COMPL_MID_X), float(x.COMPL_MID_Y)),Point(float(x.COMPL_MID_X)+1.0, float(x.COMPL_MID_Y)+1.0)]), axis=1)
df_well_xy_midcomp1.loc[df_well_xy_midcomp1["geometry"].isna(), "geometry"] = df_well_xy_midcomp1.loc[df_well_xy_midcomp1["geometry"].isna(), "geometry_midxy"]
df_well_xy_midcomp1 = gpd.GeoDataFrame(df_well_xy_midcomp1 , geometry='geometry') 
df_well_xy_midcomp1["FIELD_CODE"] = df_well_xy_midcomp1[wb_nm].map(dict(zip(df_wm[wb_nm], df_wm["FIELD_CODE"])))
df_well_xy_midcomp1["DrainLength"] = list(df_well_xy_midcomp1["geometry"].length)
df_well_xy_midcomp1["DrainLength"]=df_well_xy_midcomp1["DrainLength"].apply(lambda x: 1 if x==0 else x) ###added this line to make drainlengths as 1 for vertical wells
df_well_xy_midcomp1[w_nm] = df_well_xy_midcomp1[wb_nm].map(dict(zip(df_wm[wb_nm], df_wm[w_nm])))
df_well_xy_midcomp1[w_snm] = df_well_xy_midcomp1[composite_id].map(dict(zip(df_wm[composite_id], df_wm[w_snm])))

df_wm_temp = df_wm.copy()
df_wm_temp = df_wm_temp[[composite_id,"WELLID"]]
df_wm_temp1=df_wm_temp.drop_duplicates().groupby([composite_id]).agg({"WELLID":list})
df_wm_temp1=df_wm_temp1.reset_index()
dicttemp1 =dict(zip(df_wm_temp1[composite_id],df_wm_temp1["WELLID"]))
df_well_xy_midcomp1["WELLID"] = df_well_xy_midcomp1[composite_id].map(dicttemp1)
df_well_xy_midcomp1=df_well_xy_midcomp1.explode("WELLID")

# df_well_xy_midcomp1["WELLID"]=df_well_xy_midcomp1[composite_id].map(dict(zip(df_wm[composite_id], df_wm["CND_NAME"])))

#%%

dist_rng = [50,100,150]
df_drng_polygon = gpd.GeoDataFrame()


f_list=[]
well_list = []
pat_list = []
drng_rad_list = []
intersect_poly_list = []
intersect_area_list = []
intersect_area_frac_list = []

for field in field_list:
    
    df_field_well = df_well_xy_midcomp1[df_well_xy_midcomp1.FIELD_CODE==field]
    
    df_pat = df_pat_shp[df_pat_shp.FIELD_CODE==field]
    dict_patpoly = dict(zip(df_pat["PAT_NAME"] , df_pat["geometry"]))

    for dr in dist_rng:
        df_field_well["geometry_drn_poly"] = df_field_well["geometry"].buffer(dr)
        dict_drngbox = dict(zip(df_field_well[wb_sn] , df_field_well["geometry_drn_poly"]))
        
        tmpdf_drng_poly = gpd.GeoDataFrame()
        tmpdf_drng_poly = df_field_well[[wb_sn,composite_id,'geometry_drn_poly','DrainLength']]
        tmpdf_drng_poly["DrainageRadius"] = dr
        df_drng_polygon = df_drng_polygon.append(tmpdf_drng_poly) 
    
        for well in df_field_well[wb_sn].tolist():
            for pat in df_pat["PAT_NAME"].tolist():                
                well_poly = dict_drngbox[well]
                pat_poly = dict_patpoly[pat]
                try:
                    pat_poly=pat_poly.buffer(0)
                    intersect_poly = well_poly.intersection(pat_poly)
                    intersect_area = intersect_poly.area
                    well_poly_area = well_poly.area
                    intersect_area_frac = intersect_area/well_poly_area                   
                except:
                    print(pat)
                    intersect_poly= np.nan
                    intersect_area = np.nan   
                    well_poly_area = np.nan
                    intersect_area_frac = np.nan

                if intersect_area >0:
                    f_list.append(field)
                    well_list.append(well)
                    pat_list.append(pat)
                    drng_rad_list.append(dr)
                    intersect_poly_list.append(intersect_poly)
                    intersect_area_list.append(intersect_area)
                    intersect_area_frac_list.append(intersect_area_frac)
                    # print(field, pat, well, dr, intersect_area, " :Overlap exists")
                # else:
                #     print(field, pat, well, dr, intersect_area, " :No overlap")

       
df_pat_alloc = pd.DataFrame()
df_pat_alloc["FIELD_CODE"] = f_list
df_pat_alloc[wb_sn] = well_list
df_pat_alloc["PAT_NAME"] = pat_list
df_pat_alloc["DR"] = drng_rad_list
df_pat_alloc["OLAP_AREA"] = intersect_area_list
df_pat_alloc["OLAP_FRAC"] = intersect_area_frac_list
df_pat_alloc["D_LENGTH"]=df_pat_alloc[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1["DrainLength"])))
df_pat_alloc["geometry"] = intersect_poly_list
df_pat_alloc[wb_nm]=df_pat_alloc[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1[wb_nm])))
df_pat_alloc[w_nm]=df_pat_alloc[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1[w_nm])))
df_pat_alloc[w_snm]=df_pat_alloc[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1[w_snm])))


df_pat_alloc["WELLID"]=df_pat_alloc[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1["WELLID"])))



df_well_length=df_pat_alloc[[w_nm,w_snm,wb_sn,"D_LENGTH"]]
df_well_length.drop_duplicates(inplace=True)
df_well_len=df_well_length.groupby([w_nm])["D_LENGTH"].sum()
dict_well_len=dict(df_well_len)

df_pat_alloc["W_LEN"]=df_pat_alloc[w_nm].map(dict_well_len)

#%%%Flank well allocation factor

df_flank_AF = pd.DataFrame()

dict_field_flank_area_target ={'QAHARIR':0.2, 'SAIH RAWL':0.2,'LEKHWAIR':0.2,'ZAULIYAH':0.2,'KARIM WEST':0.2}

# df_well_xy_midcomp1_temp=df_well_xy_midcomp1[df_well_xy_midcomp1[wb_sn]=="Q-46H1"]

for field in field_list:#["SAIH RAWL"]:
    
    df_field_well1 = df_well_xy_midcomp1[df_well_xy_midcomp1.FIELD_CODE==field]
    
    df_pat1 = df_pat_shp[df_pat_shp.FIELD_CODE==field]
    target_intersect_area_frac = dict_field_flank_area_target[field]
    # dict_patpoly1 = dict(zip(df_pat["PAT_NAME"] , df_pat["geometry"]))

    for dr in [100]:#dist_rng:
        # df_field_well1["geometry_drn_poly"] = df_field_well1["geometry"].buffer(dr)
        # dict_drngbox1 = dict(zip(df_field_well1[wb_sn] , df_field_well1["geometry_drn_poly"]))
        
        # tmpdf_drng_poly1 = gpd.GeoDataFrame()
        # tmpdf_drng_poly1 = df_field_well[[wb_sn,composite_id,'geometry_drn_poly','DrainLength']]
        # tmpdf_drng_poly1["DrainageRadius"] = dr
        # df_drng_polygon1 = df_drng_polygon1.append(tmpdf_drng_poly1) 


        for well1,dfwell1 in df_field_well1.groupby([wb_sn]):
            if well1 in well_list:
                continue
            else:
                print(well1)
                comp_drain=dfwell1["geometry"].tolist()[0]
                
                
                # for pat1 in df_pat["PAT_NAME"].tolist():                
                #     well_poly1 = dict_drngbox[well1]
                #     pat_poly1 = dict_patpoly[pat1]

                for i in range(0,20,1):
                    
                    dr_mult = round(1+i*0.5,2)
                    well_poly1 = comp_drain.buffer(dr*dr_mult) # drainage radius for the well
                    df_pat1["geometry"]=df_pat1["geometry"].buffer(0)
                    df_pat1["Overlap_Area"]=df_pat1.apply(lambda x: x["geometry"].intersection(well_poly1).area,axis=1)
                    df_pat1["Overlap_Area_Frac"] = df_pat1.apply(lambda x: x["Overlap_Area"]/well_poly1.area , axis=1)
                    tot_intersect_area = round(df_pat1["Overlap_Area_Frac"].sum() ,2)
                    df_pat1["DR"]=dr
                    # pat_poly1=pat_poly1.buffer(0)
                    # intersect_poly1 = well_poly1.intersection(pat_poly1)
                    # intersect_area1 = intersect_poly1.area   
                    # well_poly_area1 = well_poly1.area
                    # intersect_area_frac1 = intersect_area1/well_poly_area1
                        
                    if  tot_intersect_area < target_intersect_area_frac: 
                        print(well1, ", dr_mult: ", dr_mult, ", Area frac: ",tot_intersect_area)
                    else:
                        print("Overlap target met. Total overlap frac: ",tot_intersect_area )
                        break          
                pat_poly_df1 = df_pat1.loc[df_pat1["Overlap_Area"]>0]
            
                if len(pat_poly_df1)>0:
                    pat_poly_df1[wb_sn] = well1
                    pat_poly_df1["AF"] = pat_poly_df1["Overlap_Area_Frac"]/(pat_poly_df1["Overlap_Area_Frac"].sum())
                    pat_poly_df1["AF"] = pat_poly_df1.apply(lambda x: x["AF"] if x["AF"] > userinput.alloc_fac_cutoff else 0, axis=1)
                    pat_poly_df1["AF"] = pat_poly_df1["AF"]/(pat_poly_df1["AF"].sum())
                    df_flank_AF = df_flank_AF.append(pat_poly_df1)
                else:
                    print(well1," : No overlap even at dr_mult of: ", dr_mult)

df_flank_AF["D_LENGTH"]=df_flank_AF[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1["DrainLength"])))
df_flank_AF[wb_nm]=df_flank_AF[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1[wb_nm])))
df_flank_AF[w_nm]=df_flank_AF[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1[w_nm])))
df_flank_AF[w_snm]=df_flank_AF[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1[w_snm])))
df_flank_AF["WELLID"]=df_flank_AF[wb_sn].map(dict(zip(df_well_xy_midcomp1[wb_sn],df_well_xy_midcomp1["WELLID"])))

df_flank_AF.rename(columns={'Overlap_Area':'OLAP_AREA','Overlap_Area_Frac':'OLAP_FRAC'},inplace=True)
df_flank_AF["W_LEN"]=df_flank_AF[w_nm].map(dict_well_len)

df_flank_AF1=df_flank_AF[['FIELD_CODE', wb_sn, 'PAT_NAME', 'DR', 'OLAP_AREA',
       'OLAP_FRAC', 'D_LENGTH', 'geometry', wb_nm, w_nm,
       w_snm, 'WELLID', 'W_LEN']]

df_pat_alloc_all = pd.concat([df_pat_alloc,df_flank_AF1])


# %% Calculating Allocation factor for each well in each pattern
for f in field_list:
    df_pat_alloc1=df_pat_alloc_all[df_pat_alloc_all["DR"]==userinput.frac_area_drainage_radius[f]]
df_pat_alloc1=df_pat_alloc1.drop(columns=[geom])
df_pat_alloc1.drop_duplicates(inplace=True)
df_alloc=pd.DataFrame()
for wb1,dfwb1 in df_pat_alloc1.groupby([w_nm]):
    dfwb1["OLAPFRAC_DRAIN"]=dfwb1["OLAP_FRAC"]*dfwb1["D_LENGTH"]
    dfwb1["DRAIN_ALLOC"]=dfwb1.apply(lambda x:x["OLAPFRAC_DRAIN"]/x["W_LEN"] if pd.isna(x["W_LEN"])==False else x["OLAPFRAC_DRAIN"],axis=1)
    for pat,dfpat in dfwb1.groupby(["PAT_NAME"]):
        dfpat["AF_temp"]=dfpat["DRAIN_ALLOC"].sum()
        df_pat1=dfpat[["PAT_NAME",w_nm,"WELLID","AF_temp"]]
        df_alloc=pd.concat([df_alloc,df_pat1])
df_alloc=df_alloc.drop_duplicates(subset=["PAT_NAME","WELLID"])
df_alloc["n_AF"]=df_alloc["AF_temp"] / df_alloc.groupby("WELLID")["AF_temp"].transform('sum')

df_alloc1=pd.DataFrame()
for wid, dfwid in df_alloc.groupby(["WELLID"]):
    a=dfwid[dfwid["n_AF"]<userinput.alloc_fac_cutoff]
    dfwid.loc[dfwid["n_AF"].idxmax(), "n_AF"]= dfwid.loc[dfwid["n_AF"].idxmax(), "n_AF"]+a["n_AF"].sum()
    dfwid["AF"]=dfwid["n_AF"].apply(lambda x: x if x>userinput.alloc_fac_cutoff else 0)
    # dfwid["n_AF"]=dfwid["n_AF"].apply(lambda x: x if x<(1-userinput.alloc_fac_cutoff) else 1)
        
    df_alloc1=pd.concat([df_alloc1,dfwid])
# df_alloc1=df_alloc1[df_alloc1["AF"]>0]
# df_alloc1=df_alloc.copy()
df_alloc1["AF"]=round(df_alloc1["AF"],2)
#%%
df_wm_temp = df_wm.copy()
df_wm_temp = df_wm_temp[[wb_sn,composite_id]]
df_wm_temp1=df_wm_temp.drop_duplicates().groupby([wb_sn]).agg({composite_id:list})
df_wm_temp1=df_wm_temp1.reset_index()
dicttemp =dict(zip(df_wm_temp1[wb_sn],df_wm_temp1[composite_id]))
df_alloc2=df_alloc1.copy()

df_alloc2[composite_id]=df_alloc2["WELLID"].map(dicttemp)
df_alloc3=df_alloc2.explode(composite_id)
df_alloc3["FIELD_CODE"]=df_alloc3["WELLID"].map(dict(zip(df_wm[wb_sn],df_wm["FIELD_CODE"])))


#%%%
# wb_sn= "WB_SHORTNAME"
w_type="C_WELL_TYPE"
pt_nm="PAT_NAME"
compl_status="COMPL_STATUS"
compl_start="COMPL_START"
df_pattern =df_alloc3.copy()
for c in ['RES_CODE', 'COMPL_MID_X','COMPL_MID_Y', 'COMPL_STATUS', 'C_WELL_TYPE']:
    df_pattern[c]=df_pattern[composite_id].map(dict(zip(df_wm0[composite_id],df_wm0[c])))
df_pattern.rename(columns={"WELLID":wb_sn,"AF":"ALLOC_FAC","FIELD_CODE":"FIELD_NAME"},inplace=True)

dict_all_inj_list={}
dict_open_inj_list={}
for pat, dfpat  in df_pattern.groupby([pt_nm]):   
    wi_all=[]
    wi_open=[]
    for i, r in dfpat.iterrows():
        if r[w_type]=="WI" or r[w_type]=="OP-WI":
            [wi_all.append(x) for x in [r[wb_sn]] if x not in wi_all]
            if r[compl_status]=="OPEN":
                [wi_open.append(x) for x in [r[wb_sn]] if x not in wi_open]
            else:
                continue
        else:
            continue
    dict_all_inj_list.update({pat:wi_all})
    dict_open_inj_list.update({pat:wi_open})
df_pattern["PAT_All_INJ"]=df_pattern[pt_nm].map(dict_all_inj_list)
df_pattern["PAT_OPEN_INJ"]=df_pattern[pt_nm].map(dict_open_inj_list)
dict_conn_inj_list={}
dict_conn_open_inj_list={}

for pat, dfpat  in df_pattern.groupby([wb_sn]):   
    wi_all=[]
    wi_open=[]
    for i, r in dfpat.iterrows():
        [wi_all.append(x) for x in r["PAT_All_INJ"] if x not in wi_all]
        # if r[compl_status]=="OPEN":
        [wi_open.append(x) for x in r["PAT_OPEN_INJ"] if x not in wi_open]
        # else:
        #     continue
        
    dict_conn_inj_list.update({pat:wi_all})
    dict_conn_open_inj_list.update({pat:wi_open})

df_pattern["CONN_ALL_INJ"]=df_pattern[wb_sn].map(dict_conn_inj_list)
df_pattern["CONN_OPEN_INJ"]=df_pattern[wb_sn].map(dict_conn_open_inj_list)

df_pattern["PAT_WB_PAIR"]=df_pattern["PAT_NAME"]+"_"+df_pattern[wb_sn]

dist_df=pd.DataFrame()
from scipy.spatial import distance_matrix
df_pattern1=df_pattern[['PAT_NAME', wb_sn, 'FIELD_NAME', 'RES_CODE', 'COMPL_MID_X','COMPL_MID_Y', 'COMPL_STATUS', 'C_WELL_TYPE',"CONN_OPEN_INJ","PAT_WB_PAIR","ALLOC_FAC"]]
for f, dff in df_pattern1.groupby(["FIELD_NAME","PAT_NAME"]):
    dfxy=dff[[wb_sn,'C_WELL_TYPE','COMPL_MID_X','COMPL_MID_Y']]
    dist=distance_matrix(dfxy.set_index([wb_sn,'C_WELL_TYPE']).values,dfxy.set_index([wb_sn,'C_WELL_TYPE']).values)
    dist=pd.DataFrame(dist,index=dfxy.set_index([wb_sn,'C_WELL_TYPE']).index,columns=dfxy.set_index([wb_sn,'C_WELL_TYPE']).index)
    dist.columns=dist.columns.get_level_values(0)
    dist.reset_index(inplace=True)
    dist.rename(columns={wb_sn:"s_"+wb_sn,w_type:"s_"+w_type},inplace=True)
    dist=pd.melt(dist,id_vars=["s_"+wb_sn,"s_"+w_type],var_name=wb_sn,value_name="DISTANCE")
    # dist["RES_CODE"]=f[1]
    dist["FIELD_NAME"]=f[0]
    dist["PAT_NAME"]=f[1]
    dist[w_type]=dist[wb_sn].map(dict(zip(dfxy[wb_sn],dfxy[w_type])))
    dist["PAT_WB_PAIR"]=dist["PAT_NAME"]+"_"+dist[wb_sn]
    dist["CONN_OPEN_INJ"]=dist[wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["CONN_OPEN_INJ"])))
    dist["ALLOC_FAC"]=dist["PAT_WB_PAIR"].map(dict(zip(df_pattern1["PAT_WB_PAIR"],df_pattern1["ALLOC_FAC"])))
    dist["COMPL_STATUS"]=dist[wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["COMPL_STATUS"])))
    dist["s_COMPL_STATUS"]=dist["s_"+wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["COMPL_STATUS"])))
    dist["COMPL_MID_X"]=dist[wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["COMPL_MID_X"])))
    dist["COMPL_MID_Y"]=dist[wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["COMPL_MID_Y"])))
    dist["s_COMPL_MID_X"]=dist["s_"+wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["COMPL_MID_X"])))
    dist["s_COMPL_MID_Y"]=dist["s_"+wb_sn].map(dict(zip(df_pattern1[wb_sn],df_pattern1["COMPL_MID_Y"])))
    dist=dist[dist[w_type]=="OP"]
    dist=dist[dist["s_"+w_type]!="OP"]
    
    
    dist_df=pd.concat([dist_df,dist])
    dist_df["WELL_PAIR_SN"]=dist_df["s_"+wb_sn]+"_"+dist_df[wb_sn]


# %% EXPORTING FILES

# output_folder_temp = "C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1"

df_drain_shp  = gpd.GeoDataFrame(df_drain_shp , geometry=geom) 
df_drain_shp.to_crs = {'init' :'epsg:4326'}
df_drain_shp.to_file(output_folder +"/01__Drain_Shapefile.shp", driver='ESRI Shapefile')
# df_drain_shp.to_file(output_folder_temp +"/01__Drain_Shapefile.shp", driver='ESRI Shapefile')

df_pat_alloc_shp = gpd.GeoDataFrame(df_pat_alloc_all , geometry='geometry')  
df_pat_alloc_shp .to_crs = {'init' :'epsg:4326'}
df_pat_alloc_shp .to_file(output_folder +"/01_IntersectPoly_well_pattern_Shapefile.shp", driver='ESRI Shapefile')   
# df_pat_alloc_shp .to_file(output_folder_temp +"/01_IntersectPoly_well_pattern_Shapefile.shp", driver='ESRI Shapefile')   

df_alloc3.to_csv(output_folder+"/"+userinput.alloc_factor_output,index=False)
# df_alloc3.to_csv(output_folder_temp+"/"+userinput.alloc_factor_output,index=False)
dist_df.to_csv(userinput.output_folder+"/"+userinput.pattern_dist_matrix, index=False)
# %%

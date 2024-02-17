"""Combining and processing pattern name and
mapping to wells for each field"""

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
import geopandas as gpd
#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput


#%%% setting code working folder
manual_data_folder =userinput.manualdata_input_folder
static_data_folder = userinput.staticdata_input_folder
dynamic_data_folder = userinput.dynamicdata_input_folder
output_folder=userinput.output_folder
# dbtables_folder = userinput.dbtables_folder

wmaster_file="*_WMASTER.xls*"
sheettoread="Pattern_Definitions"
wmaster_filedb=userinput.wmaster_output

#%%% defining column headers as variables

ab_name="Abbr Name"
field_code="FIELD_CODE"
composite_id="COMPOSITE_ID"
w_type="WELL_TYPE"

ab_name1="Abbr_Name"
dat="DATES"
uniqueid="UNIQUEID"
wellbore="WELLBORE"
wb_sn= "WELLBORE_SHORT_NAME" #changing as per new database column header
w_type="C_WELL_TYPE"
pt_nm="PAT_NAME"
compl_status="COMPL_STATUS"
compl_start="COMPL_START"

#%% importing functions py and userinput py

import userinput
import WFO_Functions as fn


#%%Reading and combining pattern definitions/mapping to each well and allocation

df_pattern = pd.DataFrame()

for root,subdirs, file in os.walk(manual_data_folder):
    for filename in fnmatch.filter(file,wmaster_file):
        
        try:
            df=pd.read_excel(os.path.join(root,filename),sheet_name = sheettoread )
            df_pattern = pd.concat([df_pattern,df])
        except:
            continue

df_pattern =df_pattern[[pt_nm, ab_name,  'ALLOC_FAC', 'PAT_MAIN_INJ', 'STOIIP',"CND_NAME"]]

df_wmaster_com=pd.read_csv(userinput.output_folder+"/"+wmaster_filedb)
df_wmaster_com=df_wmaster_com[df_wmaster_com["STIR_FLAG"]==1]
# df_wmaster_com=df_wmaster_com[df_wmaster_com[compl_status]=="OPEN"]
#%%Temperory pattern names for new fields
# temp_pat = df_wmaster_com[df_wmaster_com["FIELD_NAME"].isin(["KARIM WEST","LEKHWAIR"])]
# temp_pat=temp_pat[[wb_sn,"FIELD_RES_GROUP"]]
# temp_pat.rename(columns={wb_sn:ab_name,"FIELD_RES_GROUP":"PAT_NAME"},inplace=True)
# df_pattern1 = pd.merge(df_pattern,temp_pat,how="outer")

# df_pattern=df_pattern1.copy()
#%%
df_pattern["FIELD_NAME"]=df_pattern[ab_name].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["FIELD_NAME"])))
df_pattern["RES_CODE"]=df_pattern[ab_name].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["RES_CODE"])))
df_pattern["FIELD_RES_GROUP"]=df_pattern[ab_name].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["FIELD_RES_GROUP"])))
df_pattern["COMPL_MID_X"]=df_pattern[ab_name].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["COMPL_MID_X"])))
df_pattern["COMPL_MID_Y"]=df_pattern[ab_name].map(dict(zip(df_wmaster_com[wb_sn],df_wmaster_com["COMPL_MID_Y"])))
df_pattern.rename(columns={ab_name: wb_sn},inplace=True)

df_pattern=df_pattern.drop_duplicates()

dict_well_type={}
dict_well_status={}
for wb, dfwb in df_wmaster_com.groupby([wb_sn]):
    dfwb =dfwb.dropna(subset=[compl_start])
    if len(dfwb)>0:
        dfwb[compl_start]=pd.to_datetime(dfwb[compl_start])
        wt = dfwb.loc[dfwb[compl_start].idxmax(),w_type]
        ws=dfwb.loc[dfwb[compl_start].idxmax(),compl_status]
        dict_well_type.update({wb:wt})
        dict_well_status.update({wb:ws})
    else:continue
df_pattern[compl_status]=df_pattern[wb_sn].map(dict_well_status)
df_pattern[w_type]=df_pattern[wb_sn].map(dict_well_type)

#%%
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

#%%Distance Matrix for pattern mapping
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
# dist_df["s_PAT_NAME"]=dist_df["s_"+wb_sn].map(dict(zip(df_pattern[wb_sn],df_pattern["PAT_NAME"])))
# dist_df["PAT_NAME"]=dist_df[wb_sn].map(dict(zip(df_pattern[wb_sn],df_pattern["PAT_NAME"])))

#%%% combining pattern shapefiles exported from petrel
# df_pat_shpfile =gpd.GeoDataFrame()
# for root,subdirs, file in os.walk(static_data_folder+"/SR/PETREL_EXPORT/PATTERNPOLYGONS"):
#     for filename in fnmatch.filter(file,"*.shp"):
#         try:
#             df=gpd.GeoDataFrame.from_file(os.path.join(root,filename))
#             print(df)
#             df_pat_shpfile = gpd.GeoDataFrame(pd.concat([df_pat_shpfile,df]),geometry='geometry')
#             print(df_pat_shpfile)
#         except:
#             continue
# df_pat_shpfile["FIELD_NAME"]="SAIH RAWL"
# df_pat_shpfile.to_crs = {'init' :'epsg:4326'}
# df_pat_shpfile.to_file(static_data_folder+"/SR/PETREL_EXPORT/PATTERNPOLYGONS" +"/00_combined_pattern_polygon.shp", driver='ESRI Shapefile') 
#========================================
#Qaharir shape file polygonizing

# from shapely.ops import polygonize
# for root,subdirs, file in os.walk(static_data_folder+"/Q/PETREL_EXPORT/PatternPolygon"):
#     for filename in fnmatch.filter(file,"*.shp"):
#         # df=gpd.GeoDataFrame.from_file(os.path.join(root,filename))
#         # print(df)
#         try:
#             df=gpd.GeoDataFrame.from_file(os.path.join(root,filename))
#             print(df)
#             df.plot()
#             df["geometry"] = gpd.GeoSeries(polygonize(df.geometry)) 
#             df = gpd.GeoDataFrame(df, geometry='geometry')
#             df.plot()
#         except:
#             continue
# df.to_crs = {'init' :'epsg:4326'}
# df.to_file(static_data_folder+"/Q/PETREL_EXPORT/PatternPolygon" +"/00_Pattern Polygon_polygonized.shp", driver='ESRI Shapefile') 


#%%Function for plotting
dist_df1=dist_df[dist_df["COMPL_STATUS"]=="OPEN"]
dist_df1= dist_df1[ dist_df1["s_COMPL_STATUS"]=="OPEN"]


def create_df_for_confactor_plot(sdf,  con_factor_col_prd ="ALLOC_FAC", injname_col = "s_"+wb_sn, prdname_col = wb_sn):
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
    # sdf[con_factor_col_inj].fillna(0, inplace=True)
    sdf[con_factor_col_prd].fillna(0, inplace=True)

    sdf_pp_out = pd.DataFrame()
    for field_res, sdf_field_res in sdf.groupby(["FIELD_NAME"]):
            
        sdf1 = sdf_field_res[["Injector","Producer",con_factor_col_prd, "s_COMPL_MID_X", "s_COMPL_MID_Y","FIELD_NAME"]].reset_index(drop=True)   
        sdf1["WELL_NAME"] = sdf1["Injector"]
        sdf1["WELL_TYPE"] = "Injector"
        sdf1 = sdf1.rename(columns={"s_COMPL_MID_X":"WELL_COMPL_MID_X", "s_COMPL_MID_Y":"WELL_COMPL_MID_Y"})
        sdf1.sort_values(["Injector","Producer"], inplace=True) 
                   
        sdf2 = sdf_field_res[["Injector","Producer",con_factor_col_prd, "COMPL_MID_X", "COMPL_MID_Y","FIELD_NAME"]].reset_index(drop=True)   
        sdf2["WELL_NAME"] = sdf2["Producer"]
        sdf2["WELL_TYPE"] = "Producer"
        sdf2 = sdf2.rename(columns={"COMPL_MID_X":"WELL_COMPL_MID_X", "COMPL_MID_Y":"WELL_COMPL_MID_Y"})
        sdf2.sort_values(["Producer","Injector"], inplace=True)            
        
        sdf_pp_out_tmp = pd.concat([sdf1, sdf2])   
        sdf_pp_out = pd.concat([sdf_pp_out, sdf_pp_out_tmp])              
        print("Completed: ", field_res)

    return sdf_pp_out

dist_df_short = dist_df1[['s_'+wb_sn,wb_sn,"ALLOC_FAC","s_COMPL_MID_X", "s_COMPL_MID_Y","COMPL_MID_X", "COMPL_MID_Y",'FIELD_NAME']]
dist_df_4plot = create_df_for_confactor_plot(dist_df_short)
dist_df_4plot=dist_df_4plot.drop_duplicates()

df_pattern.dropna(how="all",inplace=True)

#%%Exporting pattern mapping file
# output_folder_temp = "C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New1"

# dist_df.to_csv(output_folder_temp+"/"+userinput.pattern_dist_matrix, index=False)
# dist_df_4plot.to_csv(output_folder_temp+"/"+"P0_pattern_dist_matrix_plot.csv", index=False)

# df_pattern.to_csv(output_folder_temp+"/"+userinput.pattern_map_output, index=False)



dist_df.to_csv(userinput.output_folder+"/"+userinput.pattern_dist_matrix, index=False)
dist_df_4plot.to_csv(userinput.output_folder+"/"+"P0_pattern_dist_matrix_plot.csv", index=False)

df_pattern.to_csv(userinput.output_folder+"/"+userinput.pattern_map_output, index=False)

print("*********************************************************************************")
print("Exported :"+userinput.pattern_map_output)
print("*********************************************************************************")


#%%
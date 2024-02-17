# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 09:01:32 2021

@author: R.Agrawal

File Description: this program is for calculation of distance matrix and get first level static connectivity factor
    
Input description: WMASTER
    
    
Output description: distance matrix dataframe later combined with WMASTER
    


"""

#%% importing libraries


import os
import sys
import pandas as pd
import numpy as np
import fnmatch
from scipy.spatial import distance_matrix
import geopandas as gpd

#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput

#%% setting file and folder location

dynamic_data_folder = userinput.dynamicdata_input_folder+"/SR"
manual_data_folder= userinput.manualdata_input_folder
static_data_folder=userinput.staticdata_input_folder

wmaster_file = userinput.wmaster_output
area_frac_shpfile = "01_IntersectPoly_well_pattern_Shapefile.shp"

#%%% defining column headers as variables


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
field_res="FIELD_RES_GROUP"
v_dist="DISTANCE"
ab_name1="WELLBORE_SHORT_NAME"#"WB_SHORTNAME"
res_code="RES_CODE"
pat_nm="PAT_NAME"
#%%Loading wmaster file that contains x, y location
df_wmaster_com=pd.read_csv(userinput.output_folder+"/"+wmaster_file)
df_wmaster_com=df_wmaster_com[df_wmaster_com["STIR_FLAG"]==1]
# df_wmaster_com=df_wmaster_com[df_wmaster_com["FIELD_NAME"].str.contains("|".join(userinput.field_list),regex=True,na=False)]
# df_wmaster_com=df_wmaster_com[df_wmaster_com[res_code].str.contains("|".join(userinput.reservoir_list),regex=True,na=False)]

# df_frac_area = gpd.GeoDataFrame.from_file(userinput.output_folder+"/"+area_frac_shpfile)
# df_frac_area1=pd.DataFrame()
# for field in df_frac_area["FIELD_CODE"].unique():
#     df_frac_temp = df_frac_area[df_frac_area["FIELD_CODE"]==field]
#     df_frac_temp1=df_frac_temp[df_frac_temp["DRNG_RADIU"]==userinput.frac_area_drainage_radius[field]]
#     df_frac_area1=pd.concat([df_frac_area1,df_frac_temp1])

# dict_pat = dict((df_frac_area1.groupby([ab_name1])[pat_nm].unique()))

# df_wmaster_com[pat_nm]=df_wmaster_com[ab_name1].map(dict_pat)


# df_wmaster_com=df_wmaster_com.explode(pat_nm)


#%%calculating distance between each well
df_wmaster_combined=df_wmaster_com
# df_wmaster_temp = df_wmaster_combined[df_wmaster_combined[field_code]=="SR"]
dist_df=pd.DataFrame()
for identifier, df_xy1 in df_wmaster_combined.groupby([field_code,res_code]):
    print(identifier)
    df_xy1["CID_temp"]=df_xy1[composite_id]+"_"+df_xy1[compl_start]
    df_xy1.sort_values([ab_name1,compl_start], inplace=True)
    df_xy=df_xy1[[ab_name1,composite_id,w_type,compl_status,compl_start,"CID_temp",compl_mid_x,compl_mid_y]]
    dist=distance_matrix(df_xy.set_index([ab_name1,composite_id,w_type,compl_status,compl_start,"CID_temp"]).values,df_xy.set_index([ab_name1,composite_id,w_type,compl_status,compl_start,"CID_temp"]).values)
    dist=pd.DataFrame(dist,index=df_xy.set_index([ab_name1,composite_id,w_type,compl_status,compl_start,"CID_temp"]).index,columns=df_xy.set_index([ab_name1,composite_id,w_type,compl_status,compl_start,"CID_temp"]).index)
    dist.columns=dist.columns.get_level_values(5)
    dist.reset_index(inplace=True)
    dist.rename(columns={ab_name1:"s_"+ab_name1,composite_id:"s_"+composite_id,w_type:"s_"+w_type,compl_status:"s_"+compl_status,compl_start:"s_"+compl_start,"CID_temp":"s_CID_temp"},inplace=True)
    dist=pd.melt(dist,id_vars=["s_"+ab_name1,"s_"+composite_id,"s_"+w_type,"s_"+compl_status,"s_"+compl_start,"s_CID_temp"],var_name="CID_temp",value_name=v_dist)
    dist[composite_id]=dist["CID_temp"].map(dict(zip(df_xy1["CID_temp"],df_xy1[composite_id])))
    dist[compl_mid_x]=dist["CID_temp"].map(dict(zip(df_xy1["CID_temp"],df_xy1[compl_mid_x])))
    dist[compl_mid_y]=dist["CID_temp"].map(dict(zip(df_xy1["CID_temp"],df_xy1[compl_mid_y])))
    dist[zn_open]=dist["CID_temp"].map(dict(zip(df_xy1["CID_temp"],df_xy1[zn_open])))
    dist[compl_status]=dist["CID_temp"].map(dict(zip(df_xy1["CID_temp"],df_xy1[compl_status])))
    dist[compl_start]=dist["CID_temp"].map(dict(zip(df_xy1["CID_temp"],df_xy1[compl_start])))
    dist[res_code]=identifier[1]
    dist[field_code]=identifier[0]
    dist_df=pd.concat([dist_df,dist])

#%%adding relvant information from WMASTER for each selected well and well from which distance is calculated
"""creating selected well (prefixed with s_) metadata and combining with distance matrix dataframe"""

s_metadata = df_wmaster_combined[[composite_id,compl_start,compl_mid_x,compl_mid_y,zn_open]].drop_duplicates() # Injector metadata
s_metadata["CID_temp"]=s_metadata[composite_id]+"_"+s_metadata[compl_start]
s_metadata=s_metadata[["CID_temp",compl_mid_x,compl_mid_y,zn_open]].drop_duplicates()
s_metadata.columns = ["s_" + x for x in s_metadata.columns]
dist_df = pd.merge(dist_df, s_metadata, on = "s_CID_temp", how = "left")

"""creating metadata for all other wells and combingin with distance matrix dataframe""" 

metadata = df_wmaster_combined[[composite_id,ab_name1,w_type,compl_start]].drop_duplicates() # Producer metadata
metadata["CID_temp"]=metadata[composite_id]+"_"+metadata[compl_start]
metadata=metadata[["CID_temp",ab_name1,w_type]].drop_duplicates()
dist_df = pd.merge(dist_df, metadata, on = "CID_temp", how = "left")

dist_df["WELL_PAIR"] = dist_df["s_"+composite_id] + "/" + dist_df[composite_id]
dist_df["WELL_PAIR_TYPE"] = dist_df["s_"+w_type] + "_" + dist_df[w_type]
dist_df[field_res] = dist_df[field_code] +"_"+ dist_df[res_code]

dist_df=dist_df.drop_duplicates()
dist_df.drop(labels=["CID_temp","s_CID_temp"],axis=1,inplace=True)
#%%Getting appropriate open zone list for Qaharir wells based on manual completion matrix shared by asset
df_Q_zn_map = pd.read_excel(manual_data_folder+"/Q/Q_RES_CODE_MAPPING.xlsx")
df_Q_zn_map_WI = df_Q_zn_map[df_Q_zn_map["s_C_WELL_TYPE"]=="WI"]
df_Q_zn_map_OP = df_Q_zn_map[df_Q_zn_map["C_WELL_TYPE"]=="OP"]
def get_zone_open_list(df,composite_id_col,zn_col):
    zn_dict={}
    for c, cdf in df.groupby(composite_id_col):
        if len(cdf)>0:
            cdf["RES_COUNT"] = cdf[zn_col].nunique()
            # cdf["RES_COMP_TYPE"] = cdf.apply(lambda x: "COMMINGLED" if x["RES_COUNT"]>1 else "DEDICATED", axis=1)
            cdf=cdf.dropna(subset=[zn_col])
            o_zone = cdf[zn_col].tolist()    
            o_zone = list(dict.fromkeys(o_zone))
            cdf[zn_col+"list"] = '[%s]' % ', '.join(map(str, o_zone))
            # print(c)
            # print(cdf[zn_col+"list"] )
            zn_dict.update(dict(zip(cdf[composite_id_col],cdf[zn_col+"list"])))
    return zn_dict

wi_dict= get_zone_open_list(df_Q_zn_map_WI,"s_"+composite_id,"s_"+zn_open)
op_dict= get_zone_open_list(df_Q_zn_map_OP,composite_id,zn_open)

dist_df["s_"+zn_open]=dist_df.apply(lambda z: wi_dict[z["s_"+composite_id]] if z["s_"+composite_id] in list(wi_dict.keys()) else z["s_"+zn_open],axis=1)
dist_df[zn_open]=dist_df.apply(lambda z: op_dict[z[composite_id]] if z[composite_id] in list(op_dict.keys()) else z[zn_open],axis=1)

# dist_df.to_csv("C:/Users/R.Agrawal/Desktop/WFH/PDO/04_Working_Folder/03_output/"+userinput.dist_matrix_output, index=False)

#%%%

dist_df.to_csv(userinput.output_folder+"/"+userinput.dist_matrix_output, index=False)

print("*********************************************************************************")
print("Exported :"+userinput.dist_matrix_output)
print("*********************************************************************************")

# %%

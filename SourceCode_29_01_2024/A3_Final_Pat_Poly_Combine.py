"""
Created on Fri Jul  1 02:11:57 2022

@author: Prateeksha.M

Description: Converts shapefiles to polygons 
"""
import geopandas as gpd
from shapely.geometry import Polygon, mapping
import pandas as pd
import glob
from pathlib import Path
import numpy as np
import os

code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)
import userinput
output_folder=userinput.output_folder
input_folder = userinput.output_folder
manual_data_folder= userinput.manualdata_input_folder
static_data_folder=userinput.staticdata_input_folder

dict_pat_folder = {"ZL": static_data_folder+'/ZL/PETREL_EXPORT/PATTERN_POLYGON', 'Q':static_data_folder+'/Q/PETREL_EXPORT/PatternPolygon', 'SR': static_data_folder+'/SR/PETREL_EXPORT/SR_Pattern_Polygons', 'LEK': static_data_folder+'/LEK/PETREL_EXPORT/PATTERN_POLYGON', 'KMW':static_data_folder+'/KMW/PETREL_EXPORT/PatternPolygon' } 
dict_pat_res = {"ZL":"GHARIF", 'Q': 'GHARIF', 'SR':'SHUAIBA', 'LEK': 'KHARAIB A N_SHUAIBA LOWER A N', 'KMW':'HAIMA'}
dict_pat_field = {"ZL": "ZAULIYAH", 'Q': 'QAHARIR', 'SR':'SAIH RAWL', 'LEK': 'LEKHWAIR', 'KMW': 'KARIM WEST'}  
field_list=["ZL", 'Q', 'SR', 'LEK', 'KMW']
dict_pat_geom ={"ZL":"3D LineString", "Q":"3D LineString", "SR":"3D LineString","LEK":"3D LineString", "KMW":"3D LineString"}
gdf_final = pd.DataFrame()
# gdf2 = pd.DataFrame()
shpname_list = []
def linestring_to_polygon(fili_shps):
    gdf_final = pd.DataFrame()
    gdf = gpd.read_file(fili_shps) #LINESTRING
    gdf['geometry'] = [Polygon(mapping(x)['coordinates']) for x in gdf.geometry]
    gdf_final = pd.concat([gdf_final,gdf])
    return gdf_final

for field_code in field_list: 
# for field_code in ["LEK"]: 
    folder = dict_pat_folder[field_code]
    res_code = dict_pat_res[field_code]
    for file in glob.glob(folder+'/*.shp'): 
        print(file)
        gdf = linestring_to_polygon(file)
        shpname_list = (Path(file).stem)
        gdf["FIELD_CODE"] = field_code
        gdf["RES_CODE"]= res_code
        gdf['PAT_NAME'] = np.array(shpname_list)
        gdf["FIELD_CODE"]=gdf.apply(lambda x:dict_pat_field[x.FIELD_CODE],axis=1)
        # gdf2 = pd.concat([gdf2,gdf])
        gdf_final = pd.concat([gdf_final,gdf]) 
gdf_final = gpd.GeoDataFrame(gdf_final, geometry='geometry')
gdf_final.to_crs = {'init' :'epsg:4326'}
gdf_final = gdf_final[['Type', 'Domain', 'Droid', 'Comment', 'ShapeName', 'Project',
       'geometry', 'FIELD_CODE', 'RES_CODE', 'PAT_NAME']]
gdf_final.drop_duplicates(subset=['PAT_NAME'], keep = 'first', ignore_index=True)     
gdf_final.to_file(output_folder+"/Pat_combined.shp", driver='ESRI Shapefile', SHPT='POLYGON')

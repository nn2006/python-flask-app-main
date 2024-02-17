# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 14:02:27 2022

@author: Manu.Ujjwal
"""


import os
import sys
import fnmatch
import pandas as pd
import numpy as np
import math
pd.set_option('display.max_columns', 1000)


#%%% setting code working folder
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput

#%% setting file and folder location

#Calculate distance
def calc_distance( x1, y1, x2, y2):
    dist = math.sqrt((x1-x2)**2+(y1-y2)**2)
    return dist

#Calculate azimuth function
def calc_azimuth_angle( x1, y1, x2, y2):
  angle = 0.0;
  dx = x2 - x1
  dy = y2 - y1
  if x2 == x1:
    angle = math.pi / 2.0
    if y2 == y1 :
      angle = 0.0
    elif y2 < y1 :
      angle = 3.0 * math.pi / 2.0
  elif x2 > x1 and y2 > y1:
    angle = math.atan(dx / dy)
  elif x2 > x1 and y2 < y1 :
    angle = math.pi / 2 + math.atan(-dy / dx)
  elif x2 < x1 and y2 < y1 :
    angle = math.pi + math.atan(dx / dy)
  elif x2 < x1 and y2 > y1 :
    angle = 3.0 * math.pi / 2.0 + math.atan(dy / -dx)
  return (angle * 180 / math.pi)

#Calculate distance
def calc_azimuth_range( azimuth_deg, search_deg):
    new_azi = azimuth_deg + search_deg
    if new_azi <0:
        new_azi = 360 - abs(new_azi)
    if new_azi >360:
        new_azi = new_azi -360       
    return new_azi 

#Calculate distance
def calc_angle_between_azimuth( a1, a2):
    if a1<=90:
        if a2 >= 270:
            del_angle = abs((a1+360)-a2)
        else:
            del_angle = abs(a1-a2)
    elif a1>=270:
        if a2 <= 90:
            del_angle = abs((a2+360)-a1)
        else:
            del_angle = abs(a1-a2)
    else:
        del_angle = abs(a1-a2)            
    return del_angle 

#Calculate whether an azimuth is within a provided range of azimuth
def calc_in_azimuth_flag(azimuth, azimuth_range_start, azimuth_range_end):
    delta_start = azimuth - azimuth_range_start
    delta_end = azimuth - azimuth_range_end
    inrange_flag = np.nan       
    if azimuth_range_end > azimuth_range_start:
        if delta_start>=0:
            if delta_end<=0:
                inrange_flag=1
            else:
                inrange_flag=0
        else:
            inrange_flag=0           
    else:
        azimuth_range_start_adj = azimuth_range_start
        azimuth_range_end_adj = azimuth_range_end +360
        if azimuth < azimuth_range_end:
            azimuth_adj = azimuth+360
        else:
            azimuth_adj = azimuth
        delta_start_adj = azimuth_adj - azimuth_range_start_adj
        delta_end_adj   = azimuth_adj - azimuth_range_end_adj
        if delta_start_adj>=0:
            if delta_end_adj<=0:
                inrange_flag=1
            else:
                inrange_flag=0
        else:
            inrange_flag=0            
    return inrange_flag
    


#iwell ='Q-88H3:WI:HSGH'
#search_angle_deg=30

def calc_azi_PCF_scaler(pwell_df_inp, search_angle_deg):
        
    pwell_df_inp["Azimuth"] = pwell_df_inp.apply(lambda x: calc_azimuth_angle( x['COMPL_MID_X'], x['COMPL_MID_Y'],x['s_COMPL_MID_X'], x['s_COMPL_MID_Y']), axis=1)    
    pwell_df_inp["AziRange_Start"] = pwell_df_inp.apply(lambda x:calc_azimuth_range( x["Azimuth"], -search_angle_deg), axis=1)
    pwell_df_inp["AziRange_End"] = pwell_df_inp.apply(lambda x:calc_azimuth_range( x["Azimuth"], search_angle_deg), axis=1)

    pwell_df = pwell_df_inp[['FIELD_RES_GROUP','s_COMPOSITE_ID', 's_C_WELL_TYPE',  'COMPOSITE_ID', 'COMPL_MID_X', 
                    'COMPL_MID_Y','s_COMPL_MID_X', 's_COMPL_MID_Y','DIST_FLAG','DISTANCE',"Azimuth","AziRange_Start","AziRange_End"]]
    
    iwell_list=[]
    iwell_azi_scaler =[]
    iwell_azi_name=[]
    iwell_azi_summary_df = pd.DataFrame()
    
    for iwell in pwell_df["s_COMPOSITE_ID"].unique():
        iwell_df = pwell_df[pwell_df["s_COMPOSITE_ID"]==iwell]
        azi_start = iwell_df["AziRange_Start"].values[0]
        azi_end = iwell_df["AziRange_End"].values[0]
        iwell_dist = iwell_df["DISTANCE"].values[0]
        iwell_azi = iwell_df["Azimuth"].values[0]
        
        pwell_df["InAziRange_flag"] = pwell_df.apply(lambda x:  calc_in_azimuth_flag(x["Azimuth"], azi_start, azi_end) , axis=1)            
        iwell_df_inAziRange = pwell_df[pwell_df["InAziRange_flag"]==1]  
        del pwell_df["InAziRange_flag"]
        iwell_df_inAziRange = iwell_df_inAziRange [iwell_df_inAziRange["DISTANCE"]<=iwell_dist]  
        
        iwell_df_inAziRange["inv_dist"] = iwell_df_inAziRange["DISTANCE"].apply(lambda x: 1/x)
        iwell_df_inAziRange["dist_score"] = iwell_df_inAziRange["inv_dist"]/iwell_df_inAziRange.groupby('COMPOSITE_ID')['inv_dist'].transform('sum')
        iwell_df_inAziRange["dist_score_n"] = iwell_df_inAziRange["dist_score"]/iwell_df_inAziRange.groupby('COMPOSITE_ID')["dist_score"].transform('max')
    
        iwell_df_inAziRange["del_Azimuth"] = iwell_df_inAziRange.apply(lambda x: calc_angle_between_azimuth( x["Azimuth"], iwell_azi), axis=1)
        iwell_df_inAziRange["azi_score"] = iwell_df_inAziRange.apply(lambda x: search_angle_deg - x["del_Azimuth"] if x["s_COMPOSITE_ID"]!=iwell else 0, axis=1)
        iwell_df_inAziRange["azi_score_n"] = iwell_df_inAziRange["azi_score"]/iwell_df_inAziRange.groupby('COMPOSITE_ID')["azi_score"].transform('max')
    
        iwell_df_inAziRange["total_score"] = iwell_df_inAziRange.apply(lambda x: x["azi_score_n"]+x["dist_score_n"], axis=1) # Optional: Total score can also include InjRateScore
        iwell_df_inAziRange["total_score_n"] = iwell_df_inAziRange["total_score"]/iwell_df_inAziRange.groupby('COMPOSITE_ID')["total_score"].transform('max')
        iwell_df_inAziRange['REF_INJECTOR'] = iwell
        iwell_df_inAziRange["PCF_SCALER_AZI"] = iwell_df_inAziRange[iwell_df_inAziRange["s_COMPOSITE_ID"]==iwell]["total_score_n"].values[0]
    
        iwell_df_inAziRange.sort_values(["del_Azimuth"], ascending =True, inplace=True)
    
        tmp_iwell = iwell_df_inAziRange[iwell_df_inAziRange["s_COMPOSITE_ID"]==iwell]
        tmp_other_iwell = iwell_df_inAziRange[iwell_df_inAziRange["s_COMPOSITE_ID"]!=iwell]
    
        #-----------------------------------------------------------------------------------------   
        iwell_list.append(iwell)
        #-----------------------------------------------------------------------------------------           
        try:
            if len(tmp_iwell)>0:
                az_scaler = round(tmp_iwell["total_score_n"].values[0],4)
            else:
                az_scaler = 1
        except:
                az_scaler = 1         
        iwell_azi_scaler.append(az_scaler)                
        #-----------------------------------------------------------------------------------------   
        try:
            if len(tmp_other_iwell)>0:  
                other_iwell_lst = list(tmp_other_iwell["s_COMPOSITE_ID"])
            else:
                other_iwell_lst =[""]
        except:
                other_iwell_lst =[""]
        iwell_azi_name.append(other_iwell_lst)
        #-----------------------------------------------------------------------------------------           
        iwell_azi_summary_df = iwell_azi_summary_df.append(iwell_df_inAziRange)
        print("Completed : ", iwell, "AziScaler: ",az_scaler , other_iwell_lst )           
    
    pwell_df_inp["PCF_SCALER_AZI"] = pwell_df_inp["s_COMPOSITE_ID"].map(dict(zip(iwell_list,iwell_azi_scaler)))
    pwell_df_inp["INJWELL_AZI"] = pwell_df_inp["s_COMPOSITE_ID"].map(dict(zip(iwell_list,iwell_azi_name)))
   
    #dist matrix type output showing PCF_scaler for each pair and the linked list of injectors in similar azimuth
    pwell_df_out = pwell_df_inp[['FIELD_RES_GROUP', 's_COMPOSITE_ID',  'COMPOSITE_ID', 'DISTANCE', 'DIST_FLAG', 'Azimuth','AziRange_Start', 'AziRange_End', 'PCF_SCALER_AZI']]
    
    #Details of calculation of PCF_AZI_SCALER
    iwell_azi_summary_df = iwell_azi_summary_df[['FIELD_RES_GROUP','COMPOSITE_ID', 'REF_INJECTOR','s_COMPOSITE_ID', 'Azimuth','del_Azimuth', 'DISTANCE','dist_score_n', 'azi_score_n', 'total_score', 'total_score_n','PCF_SCALER_AZI']]            
    
    return pwell_df_out, iwell_azi_summary_df
#%% Zone match score function

def calc_zone_match_score(szone_list, zone_list, default_zms):
    try:
        # szone_list= "[THSU]"
        # zone_list="[THSU, WANUL]"   
        
        szone_list_p = list(szone_list[1:-1].split(",")) # convert to standard list
        zone_list_p = list(zone_list[1:-1].split(",")) # convert to standard list
        szone_list_p = [x.strip() for x in szone_list_p]
        zone_list_p = [x.strip() for x in zone_list_p]
        
        unique_zone_list = list(dict.fromkeys(szone_list_p+zone_list_p))
        
        n_szone = len(szone_list_p)
        n_zone = len(zone_list_p)
        n_total = n_szone+n_zone
        n_unique = len(unique_zone_list)
        
        SI = n_unique/n_total
        ZMS = abs(1-SI)/0.5
        # print(ZMS, n_szone , n_zone )
    except:
        ZMS = default_zms
    
    return ZMS
#%%

def pat_match_score(szone_list, zone_list, default_zms):
    try:
        # szone_list_p= ['Q-48H1', 'Q-68H2', 'Q-62H1', 'Q-70H2']
        # zone_list_p=['Q-48H1', 'Q-68H2']  
        szone_list_p=szone_list
        zone_list_p=zone_list
        # szone_list_p = list(szone_list[1:-1].split(",")) # convert to standard list
        # zone_list_p = list(zone_list[1:-1].split(",")) # convert to standard list
        szone_list_p = [x.strip() for x in szone_list_p]
        zone_list_p = [x.strip() for x in zone_list_p]
        
        unique_zone_list = list(dict.fromkeys(szone_list_p+zone_list_p))
        
        n_szone = len(szone_list_p)
        n_zone = len(zone_list_p)
        n_total = n_szone+n_zone
        n_unique = len(unique_zone_list)
        
        SI = n_unique/n_total
        ZMS = abs(1-SI)/0.5
        # print(ZMS, n_szone , n_zone )
    except:
        ZMS = default_zms
    
    return ZMS
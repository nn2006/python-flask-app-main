# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 10:24:12 2021

@author: R.Agrawal

Program Info:

Inputs:
    
Outputs:
    
"""

#%%Import libraries

import os
import sys
import pandas as pd
import numpy as np
import fnmatch


#%%% define folder paths and file names

# root_folder = "//asia-pac.shell.com/tcs/bng/pt.simpl/proj/epx1/SAA-PDO/2021/04_WorkingFolder"

root_folder = "/waterflood_analytics/04_WorkingFolder"

staticdata_input_folder=root_folder+"/01_input/01_staticdata"
dynamicdata_input_folder=root_folder+"/01_input/02_dynamicdata"
manualdata_input_folder=root_folder+"/01_input/03_manualdata"
output_folder=root_folder+"/03_output/Updated_Output"
# output_folder="C:/Users/R.Agrawal/Desktop/WFH/PDO/04_output/New_input_files_based"
# dbtables_folder = root_folder+"/01_data/Data_for_WMASTER"
dbtables_folder1 = root_folder+"/01_data/ARDH19C_WF_SYSDL"


#%%% filename for tables from database

v_well="V_WELL.csv"
v_well_comp_int = "V_WELL_COMPLETION_INTERVAL.csv"
v_wb_dev = "V_WELLBORE_DEVIATION_DATA.csv"
v_mon_prod="V_COND_RES_MONTHLY_PRODUCTION.csv"


master_xy= "MASTERXY_OFM.csv"
monthly_prod = "MONTHLY_PROD.csv"
monthly_wi = "MONTHLY_INJ_OFM.csv"
deviation = "DEVIATION_OFM.csv"
conduit_status="CONDUIT_STATUS_OFM.csv"
rescode_mapping_file = "RESCODE_MAPPING_INPUT.xlsx"
SR_pattern_mapping="SR_Pattern_Mapping.xlsx"
QA_pattern_mapping= "QA_Pattern Allocation Factor_Map.xlsx"


#%% Define output file names

wmaster_output = "P0_MASTERXY_ANALYTICS.csv"
prod_inj_output1 = "P0_PROD_INJ_UNIQUEID_ANALYTICS.csv"
prod_inj_output = "P0_PROD_INJ_COMPOSITEID_ANALYTICS.csv"
pattern_map_output = "P0_PATTERN_MAP_ANALYTICS.csv"
pattern_dist_matrix="P0_pattern_dist_matrix.csv"
wct_analysis_output="P1_PROD_INJ_WCTANALYSIS.csv"
dist_matrix_output = "P1_DISTANCE_MATRIX_ANALYTICS.csv"
prd_inj_corr_output = "P1_PRD_INJ_CORR_ANALYTICS.csv"
prd_inj_corr_inpdata = "P1_PRD_INJ_CORR_INPDATA.csv"
alloc_factor_output = "P1_PAT_ALLOC_FAC.csv"
wfo_result_output = "P2_WFO_RESULT.csv"
wfo_result_plot_output="P2_WFO_RESULT_PLOT.csv"
pat_agg_output = "P2_PAT_AGG_RESULT.csv"
pat_time_agg="P2_PAT_AGG_TIME.csv"
pump_pi_output = "P0_PUMP_PI_DATA_ANALYTICS.CSV"

pat_rank_gain_output = "P2_INJ_EFF_RANK_GAIN.csv"



#%%Static pressure data file paths


SR_pressure_data= dynamicdata_input_folder+"/SR/02 Pat Pressure.xlsx"
SR_pressure_data_sheet = "Final SR Static Pressure"

KW_pressure_data = dynamicdata_input_folder+"/KW/Kmw_Pressure MASTER.xlsx"

KW_pressure_data_sheet = "data MASTER"

LEK_pressure_data = dynamicdata_input_folder+"/LEK/Master_Pressure Database_Lekhwair AN.xlsx"
LEK_pressure_data_sheet = "Mastersheet"


#%% Define field and reservoir names to run the STIR analysis on

field_list =["QAHARIR", "SAIH RAWL", "ZAULIYAH","KARIM WEST","LEKHWAIR"]
reservoir_list = ["SHUAIBA","GHARIF","HAIMA","KHARAIB A N_SHUAIBA LOWER A N"]

#%% define cutoff variables, tolerance variables and other user related inputs

"""enter distance cutoff for each field_resgroup combination. this cutoff will be used in distance matrix 
and static connectivity factor calculations
enter value in meters""" 

active_well_window_days=float(365)




#Assumptions - Distance cutoff
dict_dist_cutoff = {'QAHARIR_GHARIF':500, 'QAHARIR EAST_GHARIF':500,'SAIH RAWL_SHUAIBA':200,'ZAULIYAH_GHARIF':500,"KARIM WEST_HAIMA":300,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":200}

dict_zms_cutoff = {'QAHARIR_GHARIF':0.01, 'QAHARIR EAST_GHARIF':0.01,'SAIH RAWL_SHUAIBA':0.01,'ZAULIYAH_GHARIF':0.01,"KARIM WEST_HAIMA":0.01,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":0.01}




#Assumptions - Zone Match Score
default_zonescore = 0.33 # to be used if one or both wells in the pair dont have zone list info
default_resscore = 0.33 # to be used if one or both wells in the pair dont have res list info


#Assumptions - weightages
dict_dms_weightage = {'QAHARIR_GHARIF':1, 'QAHARIR EAST_GHARIF':1,'SAIH RAWL_SHUAIBA':1,'ZAULIYAH_GHARIF':1,"KARIM WEST_HAIMA":1,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":1}
 


dict_ams_weightage = {'QAHARIR_GHARIF':1, 'QAHARIR EAST_GHARIF':1,'SAIH RAWL_SHUAIBA':1,'ZAULIYAH_GHARIF':1,"KARIM WEST_HAIMA":1,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":1}

dict_zms_weightage = {'QAHARIR_GHARIF':1, 'QAHARIR EAST_GHARIF':1,'SAIH RAWL_SHUAIBA':1,'ZAULIYAH_GHARIF':1,"KARIM WEST_HAIMA":1,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":1}

dict_liqcorr_weightage = {'QAHARIR_GHARIF':1.5, 'QAHARIR EAST_GHARIF':1.5,'SAIH RAWL_SHUAIBA':1.5,'ZAULIYAH_GHARIF':1.5,"KARIM WEST_HAIMA":1.5,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":1.5}


dict_bhpress_weightage = {'QAHARIR_GHARIF':0, 'QAHARIR EAST_GHARIF':0,'SAIH RAWL_SHUAIBA':0,'ZAULIYAH_GHARIF':0,"KARIM WEST_HAIMA":0,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":0}


dict_inj_weightage= {'QAHARIR_GHARIF':0, 'QAHARIR EAST_GHARIF':0,'SAIH RAWL_SHUAIBA':0,'ZAULIYAH_GHARIF':0,"KARIM WEST_HAIMA":0,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":0}


dict_liq_weightage= {'QAHARIR_GHARIF':0, 'QAHARIR EAST_GHARIF':0,'SAIH RAWL_SHUAIBA':0,'ZAULIYAH_GHARIF':0,"KARIM WEST_HAIMA":0,
                    "LEKHWAIR_KHARAIB A N_SHUAIBA LOWER A N":0}


area_drainage_radius = 100 #select drainage radius from [50, 100, 150, 200, 250, 500 750] for intersecting area calculation 
frac_area_drainage_radius={"SAIH RAWL":100,"QAHARIR":100,"ZAULIYAH":100,"KARIM WEST":100,"LEKHWAIR":100}
fldresgrp_hor_wells=['SAIH RAWL_SHUAIBA']#,"LEKHWAIR_KHARAIB","LEKHWAIR_SHAMMAR","LEKHWAIR_SHUAIBA LOWER","LEKHWAIR_SHUAIBA UPPER"]
#Input Initial Reservoir Pressure
dict_res_press={"SAIH RAWL":15000,"QAHARIR":20000,"ZAULIYAH":30000}

#defining injector centric connectivity factor cutoff for injection efficiency calculation
# producer wells with connectivity factor < cutoff will be excluded from injector efficiency calculation
# this is because these wells are expected to have lesser impact of the injection

inj_cutoff_eff = 0.1
alloc_fac_cutoff=0.1  




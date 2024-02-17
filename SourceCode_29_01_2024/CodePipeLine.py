# -*- coding: utf-8 -*-
"""
Created on Mon Jan  2 12:59:38 2022

@author: Manu.Ujjwal
"""

#%% 1. IMPORT PYTHON LIBRARIES
#=================================================================================================

import os
import sys
import numpy as np
import pandas as pd
from pandas import datetime
from dateutil.relativedelta import*
import time
 
pd.set_option('display.max_columns', 100)
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings("ignore")
 
#% 2. DEFINE FOLDER PATHS AND DATA TABLE NAMES
#=================================================================================================

#Set code folder path
code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)

import userinput
output_folder= userinput.output_folder


def export_runlog(file_run_list, runtime_list, rundate_list):
    df_run_log = pd.DataFrame()
    df_run_log["File"] = file_run_list
    df_run_log["Run Time (min)"] = runtime_list
    df_run_log["Run Timestamp"] = rundate_list
    runlog_filename = "RunLog_"+ datetime.now().strftime("%d-%m-%Y_%Hh%Mm%Ss")+".csv"
    df_run_log.to_csv(output_folder+"/"+runlog_filename, index=False)
    print(df_run_log)
    return df_run_log


#The below list is only for reference - Any new code file addition/ resequencing should be done in 
#in the code sequence section
codefile_list = [
"deviation_combined.py",
"combining_basic_files.py",
"QA_res_code_mapping.py",
"high_WCT_Wells.py",
"1_Well_Traj_ShapeFile.py",
"1_Inj_Prd_Corr.py",
"1_PressData_Processing.py",
"02a_distance_matrix.py",
"2_WFO_Main.py"
]


#%% CODE SEQUENCE 
#=================================================================================================
#Initialize timestamps for recording each codefile run times and duration
t_start = time.time()
t_last_step = time.time()

#Define empty lists for run log information
file_run_list=[]
runtime_list=[]
rundate_list=[]

try:
    import A0_DB_Connect_Download
    print("A0_DB_Connect_Download.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("A0_DB_Connect_Download.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)

#-----------------------------------------------------------------------------------------------
try:
    import A1_data_processing_1
    print("A1_data_processing_1.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("A1_data_processing_1.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)

try:
    import A2_pattern_map_processing
    print("A2_pattern_map_processing.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("A2_pattern_map_processing.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)



try:
    import A3_Final_Pat_Poly_Combine
    print("A3_Final_Pat_Poly_Combine.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("A3_Final_Pat_Poly_Combine.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)    
   
#-------------------------------------------------------------------------------------------------

#%%
try:
    import B1_Well_Traj_ShapeFile
    print("B1_Well_Traj_ShapeFile.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("B1_Well_Traj_ShapeFile.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)  
#-------------------------------------------------------------------------------------------------


try:
    import B2_distance_matrix
    print("B2_distance_matrix : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("B2_distance_matrix.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)  
#-------------------------------------------------------------------------------------------------

try:
    import B3_PressData_Processing
    print("B3_PressData_Processing.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("B3_PressData_Processing.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)      
#-------------------------------------------------------------------------------------------------
#%%

try:
    import B4_Calc_DrainageArea_Overlap
    print("B4_Calc_DrainageArea_Overlap.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("B4_Calc_DrainageArea_Overlap.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)


try:
    import B4a_Calc_DrainageArea_Pattern_Overlap
    print("B4a_Calc_DrainageArea_Pattern_Overlap.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("B4a_Calc_DrainageArea_Pattern_Overlap.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)

try:
    import B5_Inj_Prd_Corr
    print("B5_Inj_Prd_Corr.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("B5_Inj_Prd_Corr.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)


#%%

#-------------------------------------------------------------------------------------------------

try:
    import C1_WFO_Main
    print("C1_WFO_Main.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("C1_WFO_Main.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)

#---------------------------------------------------------------------------


#%%
try:
    import C3_WFO_Result_Analysis
    print("C3_WFO_Result_Analysis.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("C3_WFO_Result_Analysis.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)

try:
    import C4_Pattern_Zone_Analysis
    print("C4_Pattern_Zone_Analysis.py : Completed : t_elapsed (min):", round((time.time()-t_start)/60,2), ", Delta_t_elapsed (min):", round((time.time()-t_last_step)/60,2))
    file_run_list.append("C4_Pattern_Zone_Analysis.py")
    runtime_list.append(round((time.time()-t_last_step)/60,2))
    rundate_list.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    t_last_step = time.time()
except NameError as e:
    print(e)
    export_runlog(file_run_list, runtime_list, rundate_list)




export_runlog(file_run_list, runtime_list, rundate_list)

print("*****************************************************************************************")
print("All Files Run.")
print("*****************************************************************************************")



# %%

"""
Created on Mon Oct  31 02:11:57 2022

@author: Prateeksha.M 

Description: Creates two files of perforated zones in each pattern
input: 

"""


import pandas as pd
import glob
from pathlib import Path
import numpy as np
import os


composite_id='COMPOSITE_ID'
pt_nm='PAT_NAME'

code_folder = "/waterflood_analytics/04_WorkingFolder/02_code"
os.chdir(code_folder)
import userinput
#output_folder=userinput.output_folder
input_folder = userinput.output_folder
manual_data_folder= userinput.manualdata_input_folder
static_data_folder=userinput.staticdata_input_folder
output_folder=userinput.output_folder
# output_folder=r'C:\Users\prateeksha.m\Desktop\PDO_WFO\03_output'

wfo_result_df = pd.read_csv(input_folder+"/"+'P2_WFO_RESULT.csv')
injector_df = wfo_result_df[['s_'+composite_id, 's_COMPL_STATUS', 's_ZONE_OPEN', pt_nm, 'ACTIVE_PAIR_FLAG']]
injector_df = injector_df[injector_df.ACTIVE_PAIR_FLAG != 0]
injector_df = injector_df.assign(s_ZONE_OPEN=injector_df['s_ZONE_OPEN'].str.split(',')).explode('s_ZONE_OPEN')
injector_df = injector_df.assign(s_ZONE_OPEN=injector_df['s_ZONE_OPEN'].str.replace("[\]\[]",''))
injector_df['s_ZONE_OPEN'] = injector_df['s_ZONE_OPEN'].str.strip()
injector_df['INJ+s_ZONE_OPEN+PAT_NAME'] = injector_df['s_'+composite_id]+' + '+injector_df['s_ZONE_OPEN']+' + '+injector_df[pt_nm]
#injector_df = injector_df.drop_duplicates(subset=['INJ+s_ZONE_OPEN'])

prod_df = wfo_result_df[[composite_id, 'COMPL_STATUS', 'ZONE_OPEN', pt_nm, 'ACTIVE_PAIR_FLAG']]
prod_df = prod_df[prod_df.ACTIVE_PAIR_FLAG != 0]
prod_df = prod_df.assign(ZONE_OPEN=prod_df['ZONE_OPEN'].str.split(',')).explode('ZONE_OPEN')
prod_df = prod_df.assign(ZONE_OPEN=prod_df['ZONE_OPEN'].str.replace("[\]\[]",''))
prod_df['ZONE_OPEN'] = prod_df['ZONE_OPEN'].str.strip()
prod_df['PROD+ZONE_OPEN+PAT_NAME'] = prod_df[composite_id]+' + '+prod_df['ZONE_OPEN']+ ' + '+prod_df[pt_nm]
#prod_df = prod_df.drop_duplicates(subset=['PROD+ZONE_OPEN'])

injector_df.to_csv(output_folder+"/P2_INJECTOR_ZONE_ANALYSIS.csv", index=False)
prod_df.to_csv(output_folder+"/P2_PRODUCER_ZONE_ANALYSIS.csv", index=False)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 15:30:47 2023

@author: inragh
"""

import pandas as pd
import numpy as np
import datetime
import matplotlib as mlt
import cx_Oracle as oc
import time
import os

# Config details for Waterflood Dataviews

username = 'WH_WATERFLOOD'

password = 'ora#wat22wh'
dsn = 'ARDH19C.WORLD'
port = 1521
encoding = 'UTF-8'
server='mus-dbc2-312.pdo.shell.om'

filepath ="/waterflood_analytics/04_WorkingFolder/01_data/ARDH19C_WF_SYSDL/"

#%%% Extracting latest data from Oracle (OFM) database and saving it in individual files
dsn_tns = oc.makedsn(server, port, service_name=dsn)
connection = oc.connect(user = username, password = password, dsn = dsn_tns, encoding = encoding) # Establishing connection
cursor = connection.cursor()
print("connected to Oracle")   

tables = ["V_WELL","V_WELL_COMPLETION_INTERVAL","V_WELLBORE_DEVIATION_DATA","V_COND_RES_MONTHLY_PRODUCTION"]
for t in tables: 
    SQL = "select * from WH_WATERFLOOD."+t
    a = cursor.execute(SQL).fetchall()
    df=pd.DataFrame((np.array(a)), columns = [row[0] for row in cursor.description])
    df.to_csv(filepath+t+".csv")

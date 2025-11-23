# -*- coding: utf-8 -*-
"""
Created on Tue Sep 12 10:30:17 2023

@author: Chi Nguyen

This code is used to disagregate daily data to hourly data using pattern from nearby hourly station

Input: 
    path: direction to daily and hourly rainfall data files (CSV format)
    st_daily: daily gauge station number
    st_hourly: hourly gauge station number
    
Output:
    fout: direction to the output file
    
*** the format of csv file should match with the sample files provided so that the script can run without errors***

"""

from pathlib import Path
from datetime import datetime
from datetime import date, timedelta

import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset

import os
import time
import csv

import rasterio
from pathlib import Path
from pyproj import Transformer
import sys, os, re, json, math

#%%
#
def h2d(hourly):
    # Select totals
    exclude_pat = "AWS_FLAG|TIME_OF_CLOSEST|QAQC|SUSPICIOUS"
    cc = [cn for cn in hourly.columns \
            if not (re.search(exclude_pat, cn) or (cn in ["", None]))]

    # Eliminate negative values
    hh = hourly.loc[:, cc]
    hh[hh<0] = np.nan

    # Compute daily total (gives nan if one hourly value is missing)
    daily = hh.rolling(24).sum()

    # Compute max of quality flags
    pat = "QAQC|QUALITY"
    cc = [cn for cn in hourly.columns if re.search(pat, cn)]
    qflags = hourly.loc[:, cc].rolling(24).max().fillna(0)
    daily.loc[:, cc] = qflags.astype(int)

    idx = daily.index.hour==8
    df = daily.loc[idx]
    df.index = df.index + pd.DateOffset(hours=1)

    # Adjust column units
    df.columns = [f"{re.sub('mm/hr', 'mm/day', cn)}" \
                        for cn in df.columns]

    return df

#
def d2h(st_daily, st_hourly, start, end):
    
    
    #read daily rain data
    path = r"Z:\work\0_Common_Data\6_ClimateGridded\4_CSIROGrids\0_Working\ANUSPLIN44\workarea\working\Chi\3_ANUSLIN_trial\2_Working\0_Script\Script_pipeline_paper\Sample"

    #read cleaned rainfall data
    filename = st_daily + '_rainfall_daily_clean.csv' #change the file name
    fp = os.path.join(path, filename)
    
    # identify delimiter
    read = pd.read_csv(fp, header = 0, sep=None, engine='python', iterator=True)
    sep = read._engine.data.dialect.delimiter
    read.close()    
    #read data
    rain = pd.read_csv(fp, header = 0, sep = sep, index_col = 0, parse_dates=True)   
    
    ### read the daily rainfall values
    dr = pd.DataFrame(rain["PRECIPITATION[mm/day]"])
    mask = (dr.index >= start) & (dr.index <= end)  #mask out the data using start and end dates
    dr_test = pd.DataFrame(dr.loc[mask])  # daily rainfall
    dr_test.index = dr_test.index + pd.to_timedelta('9h')
    dr_test.iloc[:,0][dr_test.iloc[:,0] < 0] = np.nan # change negative value to 0
    dr_test.iloc[:,0][dr_test.iloc[:,0] >= 1500] = np.nan #replace the error rainfall values if higher than the threshold

    ### read hourly rainfall from nearby station

    #read cleaned rainfall data
    filename = st_hourly + '_rainfall_hourly_clean_update.csv' #change the file name
    fp = os.path.join(path, filename)
    
    # identify delimiter
    read = pd.read_csv(fp, header = 0, sep=None, engine='python', iterator=True)
    sep = read._engine.data.dialect.delimiter
    read.close()
    
    #read data
    rain = pd.read_csv(fp, header = 0, sep = sep, index_col = 0, parse_dates=True)   

    wd = pd.DataFrame(rain["PRECIPITATION[mm/hr]"])
    mask = (wd.index >= start) & (wd.index <= end)  #mask out the data using start and end dates
    wd_test = pd.DataFrame(wd.loc[mask])  #remove df
    
    r2d = h2d(wd_test).round(3) # roudn to 3 decimal places
    
    ### disaggregate ratio  
    r = (dr_test.iloc[:,0]/r2d.iloc[:,0]).to_frame()
    r = r.replace([np.inf, -np.inf], 0)
    #r = r.fillna(0)
    
    da = pd.DataFrame([])
    # check which series is longer, [daily data] or [sum hourly to daily]
    rstart = pd.to_datetime(max(dr_test.index[0], r2d.index[0]),format = "%Y-%m-%d %H:%M:%S")
    rend = pd.to_datetime(min(dr_test.index[-1], r2d.index[-1]),format = "%Y-%m-%d %H:%M:%S")
    
    td = pd.date_range(start = rstart, end = rend, freq='D')
    ### disaggregate daily to hourly data using the radar patterns
    
    for j in range(1,td.shape[0]):       
        st = rstart + timedelta(days=j) - pd.to_timedelta('1day')
        et = st + pd.to_timedelta('23h')

        # disaggregate dataframe
        disg = wd_test.loc[st:et]*r.loc[st + pd.to_timedelta('1day')][0]
        da = pd.concat([da,disg])
        
        if (r2d.loc[st + pd.to_timedelta('1day')][0] == 0 or np.isnan(r2d.loc[st + pd.to_timedelta('1day')])[0]) and dr_test.loc[st + pd.to_timedelta('1day')][0] >= 0:
            da.loc[st:et] = dr_test.loc[st + pd.to_timedelta('1day')][0]/24 
          
    da = da.round(2)  
        
    return da 
    
    
    
#%% working
 
start = pd.to_datetime('2007-01-01 00:00', format = "%Y-%m-%d %H:%M:%S") 
end = pd.to_datetime('2022-12-31 23:00', format = "%Y-%m-%d %H:%M:%S")   
    
# disaggregate daily to hourly using the hourly pattern from a nearby station
st_daily = "58127"
st_hourly = "H058206"

da = d2h(st_daily, st_hourly, start, end)  

# save data
fout = r"Z:\work\0_Common_Data\6_ClimateGridded\4_CSIROGrids\0_Working\ANUSPLIN44\workarea\working\Chi\3_ANUSLIN_trial\2_Working\0_Script\Script_pipeline_paper\Sample"

filename = str(st_daily) + '_rainfall_hourly_clean_update.csv'
fp = os.path.join(fout,filename)
print(f"\nSaving file to {fp}")
da.to_csv(fp,  sep=',', mode='w',index=True, header=True) 


# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 14:08:46 2023

@author: Chi Nguyen

Script to generate input .dat file for running ANUSPLIN

- Input:
    raingauge: Shapefile with rai ngauges as pointa and extracted DEM values at each gauge
    path_in: direction to the folder with all the hourly rainfall data at different gauge stations

- Output:
    path_out: direction to the folder output
    
"""

#os.system("cmd /k {command}")

from datetime import datetime
from datetime import date, timedelta

import numpy as np
import pandas as pd

import os
import time
import csv

import rasterio
import xarray as xr
import rioxarray as rxr
import matplotlib.pyplot as plt
from pathlib import Path
from skimage import filters as im_filters
import geopandas as gpd
from shapely.geometry import Point, LineString
from shapely import wkt
from shapely import geometry


#%% functions

def extract_hourly_rain(start, end, raingauge):
        
    ### daily precipitation
    stationid_list = raingauge["STATIONID"]
    
    raindata = pd.DataFrame({})  #create an empty dataframe
    
    for i in stationid_list:
        stationid = i
            
        path_in = r"Z:\work\0_Common_Data\6_ClimateGridded\4_CSIROGrids\0_Working\ANUSPLIN44\workarea\working\Chi\3_ANUSLIN_trial\2_Working\0_Script\Script_pipeline_paper\Sample"
        
        #read cleaned rainfall data
        filename = str(stationid) + '_rainfall_hourly_clean_update.csv'
        fp = os.path.join(path_in, filename)
        
        #skip if stationid is not in the folder
        if os.path.exists(fp) == False:
            continue
        
        #read data
        rain = pd.read_csv(fp, header = 0, sep = ",", index_col = 0, parse_dates=True)   

        wd = pd.DataFrame(rain["PRECIPITATION[mm/hr]"])
        mask = (wd.index >= start) & (wd.index <= end)  #mask out the data using start and end dates
        wd_test = pd.DataFrame(wd.loc[mask])  #remove df

        #create a dataframe with time from the start to end date
        df = pd.DataFrame(index = pd.date_range(start, end, freq='H'))
        df_save = pd.merge(df, wd_test, left_index = True, right_index = True, how = 'left')  #pd.concat
        df_save = df_save.replace(r'^\s*$', np.nan, regex=True)
        df_save.rename(columns = {"PRECIPITATION[mm/hr]":stationid}, inplace = True)
        
        # count nan and remove station with all nan data in a day
        num_nan = df_save.iloc[:,0].isna().sum()
        
        if num_nan == df_save.shape[0]:   #skip the station if all values are nan
            continue
        
        else:
        
            df_save = df_save.fillna(float(-99.9)).astype(float).round(2)  #round to 1 decimal place
        
            #save data to dataframe
            raindata = pd.concat([raindata, df_save], axis = 1)
            
            #replace nan values with -99.9
            
    raindata_inv = raindata.T #transpose data
    raindata_inv['STATIONID'] = raindata_inv.index #add STATIONID column to be matched with the gauge_coord dataframe
        
    #read Longitude, Latitide and DEM data
    gauge_coord = raingauge[raingauge["STATIONID"].isin(raindata_inv.index)]
    gauge_coord = gauge_coord[["STATIONID","LONGITUDE_","LATITUDE_d","DEM_5km"]]  
    
    #merge two dataframe into a correct order as the input in .dat
    merged_df = gauge_coord.merge(raindata_inv, how = 'left', on = ['STATIONID'])
    
    #drop all nan values
    #merged_df = merged_df.dropna(axis=0)

    return merged_df


# extract rainfall data from the cleaned dataset 

#%% input data, change direction and time frame 

### read the raingauge stations file

path_out = r"Z:\work\0_Common_Data\6_ClimateGridded\4_CSIROGrids\0_Working\ANUSPLIN44\workarea\working\Chi\3_ANUSLIN_trial\2_Working\0_Script\Script_pipeline_paper\Sample"

# use the shape file with extracted DEM values
raingauge = gpd.read_file(r"Z:\work\0_Common_Data\6_ClimateGridded\4_CSIROGrids\0_Working\ANUSPLIN44\workarea\working\Chi\3_ANUSLIN_trial\2_Working\0_Script\Script_pipeline_paper\Sample\raingauges_hourly_2007_RCD_ECD_d2h.shp")

# time frame

start_ts = pd.to_datetime('2017-01-01 00:00', format = "%Y-%m-%d %H:%M")  # MM/DD/YYYY HH:MM:SS
end_ts = pd.to_datetime('2017-01-02 00:00', format = "%Y-%m-%d %H:%M")  # MM/DD/YYYY HH:MM:SS


total_days = int((end_ts-start_ts).days)

for i in range(0,total_days):
    
    # select start and end hour for each daily time step 
    start = start_ts + timedelta(days = i)  #pd.DatetimeOpset()
    end = start + timedelta(hours = 23)
    
    #generate input file for each date
    merged_df = extract_hourly_rain(start, end, raingauge)
    
    #save data file for each day

    filename = "Richmond_hourly_rain_" + start.strftime("%Y%m%d") + ".dat"
    fp = os.path.join(path_out,filename)
    print(f"\nSaving file to {fp}")
    merged_df.to_csv(fp,  sep='\t', mode='w',index=False, header=False) 
    
    

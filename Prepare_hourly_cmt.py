# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 10:28:17 2023

@author: ngu204

Prepare the ANU command file (.cmd) associated with the input file for SPLINE and LAPGRD programs
"""

#%%

from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import date, timedelta
import os
import geopandas as gpd
import matplotlib.pyplot as plt 


#%% DO NOT EDIT THE LINE NUMBER because it is the format in the ANUSPLIN programs

def SPLINE_cmt(spl_sample, start, path_dat, path_out):
    
    """
    generate the SPLINE cmt file for each input .dat file (24 hourly rainfall values)
    start: start time of the input file
    spl_sample: sample command file 
   
    """
    # read the sample file
    fo = open(spl_sample, 'r+')    
    #spline_cmt = fo.readlines()    
    spline_cmt = fo.read().splitlines()
    fo.close()
    
    # read the input dat file
    filename = "Richmond_hourly_rain_" + start.strftime("%Y%m%d") + ".dat"
    fp = os.path.join(path_in,filename)
    rain_input = pd.read_csv(fp, header = None, delim_whitespace=True)
        
    # create a new txt file, edit and save as a new cmt file
    cmt_name  = "Richmond_hourly_rain_" + start.strftime("%Y%m%d")
    spline_cmt[0] = cmt_name
    spline_cmt[12] = str(24)
    spline_cmt[16] = path_dat + cmt_name + ".dat"
    spline_cmt[20] = str(int(rain_input.shape[0]*0.9)) #number of knots is 80% of the total station
    
        #result files
    spline_cmt[21] = path_out + cmt_name + ".not" #not
    spline_cmt[22] = ''
    spline_cmt[23] = ''
    spline_cmt[24] = path_out + cmt_name + ".res" #res
    spline_cmt[25] = ''
    spline_cmt[26] = ''
    spline_cmt[27] = path_out + cmt_name + ".sur" #sur
    spline_cmt[28] = path_out + cmt_name + ".cov" #cov
    spline_cmt[29] = ''
    spline_cmt[30] = path_out + cmt_name + ".crv" #crv
    spline_cmt[31] = path_dat + cmt_name + ".dat"
    
    return spline_cmt

    
def LAPGRD_cmt(lap_sample, start, path_in, path_out):
    """
    generate the LAPGRD cmt file for each input .dat file (24 hourly rainfall values)
    start: start time of the input file
    lap_sample: sample command file 
   
    """
    fo = open(lap_sample, 'r+')       
    lapgrd_cmt = fo.read().splitlines()
    fo.close()
        
    # create a new txt file, edit and save as a new cmt file
    cmt_name  = "Richmond_hourly_rain_" + start.strftime("%Y%m%d")
    
    #lapgrd_cmt[0] = path_in + cmt_name + ".sur" #sur
    lapgrd_cmt[0] = path_in + "rain2_" + start.strftime("%Y%m%d") + ".sur" #sur
    
    #lapgrd_cmt[4] = path_in + cmt_name + ".cov" #cov
    
        #result files
    for i in range(15,39):
        lapgrd_cmt[i] = path_out + cmt_name + "_" + f'{i-15:02d}' + ".asc" #not
 
    return lapgrd_cmt

#%% 
path_save1 = "/datasets/work/lw-resilient-nr/work/0_Common_Data/6_ClimateGridded/4_CSIROGrids/0_Working/ANUSPLIN44/workarea/working/Chi/3_ANUSLIN_trial/2_Working/17_Richmond_hourly_splines_5km_a4k_badzeroflag/1_cmt/0_spline"
path_save2 = "/datasets/work/lw-resilient-nr/work/0_Common_Data/6_ClimateGridded/4_CSIROGrids/0_Working/ANUSPLIN44/workarea/working/Chi/3_ANUSLIN_trial/2_Working/17_Richmond_hourly_splines_5km_a4k_badzeroflag/1_cmt/1_lapgrd"
#%% read the sample .cmt file
# read a sample file
spl_sample = "/datasets/work/lw-resilient-nr/work/0_Common_Data/6_ClimateGridded/4_CSIROGrids/0_Working/ANUSPLIN44/workarea/working/Chi/3_ANUSLIN_trial/2_Working/4_Splines_v1/Hourly_splines_test1/0_input/Sample_2/daily_ANU_4k.cmt"
lap_sample = "/datasets/work/lw-resilient-nr/work/0_Common_Data/6_ClimateGridded/4_CSIROGrids/0_Working/ANUSPLIN44/workarea/working/Chi/3_ANUSLIN_trial/2_Working/17_Richmond_hourly_splines_5km_a4k_badzeroflag/lapgrd_Richmond_hourly_rain_20170330.cmt"

#%%
# create a cmt based on the input file
path_out1 = 'Z:\\work\\0_Common_Data\\6_ClimateGridded\\4_CSIROGrids\\0_Working\\ANUSPLIN44\workarea\\working\\Chi\\3_ANUSLIN_trial\\2_Working\\17_Richmond_hourly_splines_5km_a4k_badzeroflag\\4_sur\\'
path_out2 = 'Z:\\work\\0_Common_Data\\6_ClimateGridded\\4_CSIROGrids\\0_Working\\ANUSPLIN44\workarea\\working\\Chi\\3_ANUSLIN_trial\\2_Working\\17_Richmond_hourly_splines_5km_a4k_badzeroflag\\2_out\\1_lapgrd\\'
path_in = "/datasets/work/lw-resilient-nr/work/0_Common_Data/6_ClimateGridded/4_CSIROGrids/0_Working/ANUSPLIN44/workarea/working/Chi/3_ANUSLIN_trial/2_Working/17_Richmond_hourly_splines_5km_a4k_badzeroflag/0_input"
path_dat = "Z:\\work\\0_Common_Data\\6_ClimateGridded\\4_CSIROGrids\\0_Working\\ANUSPLIN44\workarea\\working\\Chi\\3_ANUSLIN_trial\\2_Working\\17_Richmond_hourly_splines_5km_a4k_badzeroflag\\0_input\\"

# time frame

start_ts = pd.to_datetime('2017-01-01 00:00', format = "%Y-%m-%d %H:%M")  # MM/DD/YYYY HH:MM:SS
end_ts = pd.to_datetime('2017-01-02 00:00', format = "%Y-%m-%d %H:%M")  # MM/DD/YYYY HH:MM:SS

total_days = int((end_ts-start_ts).days)

for i in range(0,total_days):
    
    # select start and end hour for each daily time step 
    start = start_ts + timedelta(days=i)
    
    # generate SPLINE cmt file 
    spline_cmt = SPLINE_cmt(spl_sample, start, path_dat, path_out1)
    # write a new cmt file
    cmt_file = os.path.join(path_save1, "spline_" + "Richmond_hourly_rain_" + start.strftime("%Y%m%d") + ".cmt")
    with open(cmt_file, 'w') as file:
        file.writelines("%s\n" % item for item in spline_cmt)
        
    # generate LAPGRD cmt file
    lapgrd_cmt = LAPGRD_cmt(lap_sample, start, path_out1, path_out2)
    # write a new cmt file
    cmt_file = os.path.join(path_save2, "lapgrd_" + "Richmond_hourly_rain_" + start.strftime("%Y%m%d") + ".cmt")
    with open(cmt_file, 'w') as file:
        file.writelines("%s\n" % item for item in lapgrd_cmt)
        
#%% 
"""open cmd window and run the model"""
# run the code to run the spline cmt files

# time frame
# start_ts = pd.to_datetime('2007-01-01 00:00', format = "%Y-%m-%d %H:%M")  # MM/DD/YYYY HH:MM:SS
# end_ts = pd.to_datetime('2022-12-31 23:00', format = "%Y-%m-%d %H:%M")  # MM/DD/YYYY HH:MM:SS

# total_days = int((end_ts-start_ts).days)

# go to the spline cmt folder
# 1st step: run the spline loop
os.chdir('Z:\\work\\0_Common_Data\\6_ClimateGridded\\4_CSIROGrids\\0_Working\\ANUSPLIN44\\workarea\\working\\Chi\\3_ANUSLIN_trial\\2_Working\\17_Richmond_hourly_splines_5km_a4k_badzeroflag\\1_cmt\\0_spline')
for i in range(0,total_days):
    start = start_ts + timedelta(i)
    cmd_line = "SPLINE <" + "spline_" + "Richmond_hourly_rain_" + start.strftime("%Y%m%d") + ".cmt" + "> " + "spline_" + start.strftime("%Y%m%d") + ".log"
    os.system("cmd /k " + cmd_line)

#%%
# 2nd step: go to the lapgrd cmt folder    
#run the lapgrd loop
os.chdir('Z:\\work\\0_Common_Data\\6_ClimateGridded\\4_CSIROGrids\\0_Working\\ANUSPLIN44\\workarea\\working\\Chi\\3_ANUSLIN_trial\\2_Working\\17_Richmond_hourly_splines_5km_a4k_badzeroflag\\1_cmt\\1_lapgrd')
for i in range(0,total_days):
    start = start_ts + timedelta(i)
    cmd_line = "LAPGRD <" + "lapgrd_" + "Richmond_hourly_rain_" + start.strftime("%Y%m%d") + ".cmt" + "> " + "lapgrd_" + start.strftime("%Y%m%d") + ".log"
    os.system("cmd /k " + cmd_line)  
    


    
 

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 22, 2020
Functions to validate data

@author: vyachez
"""
# Imports
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime
from datetime import timedelta

import importlib
import strack_env as env
importlib.reload(env)


def missing_minutes(master_df, path):
    '''
        Creates .csv with missing minutes stats to analyze gaps
    '''
    # uniques values 
    u_days = np.unique(master_df.index.date).shape[0]
    u_ticks = len(master_df['ticker'].unique())
    # ticker-day records
    recs = u_ticks*u_days
    
    # iterating through all minutes from 8.00 am
    start_t = datetime.datetime(1999,1,1,8,0)
    miss_t_observ = []
    for m in tqdm(range(1, 8*60+1)):
        next_m = start_t + timedelta(minutes=m)
        
        # count of records with specific time
        time_present = master_df.loc[master_df.index.time == \
                                     next_m.time()].shape[0]
        
        # missing records
        missing_rec = recs - time_present
        missing_pct = round((recs - time_present)/(recs)*100,1)
        
        miss_t_observ.append([next_m.time(),
            missing_pct, missing_rec])
    
    # saving to csv
    miss_t_observ_df = pd.DataFrame(miss_t_observ,columns=["minute","missing_%",
                                                           "missing_recs"])
    miss_t_observ_df.to_csv(path+"strack_data/missing_times.csv", index="False")
    
    # returning dataframe
    return miss_t_observ_df

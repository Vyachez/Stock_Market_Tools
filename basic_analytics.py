#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 9

Basic functions to analyze market data

@author: vyachez
"""
# Imports
import numpy as np
import pandas as pd
import importlib
import datetime
from datetime import timedelta

import strack_utils as u
importlib.reload(u)
import strack_env as env
importlib.reload(env)
import stock_collector as collector
importlib.reload(collector)
import strack_risk_model as risk_mod
importlib.reload(risk_mod)
import strack_delivery as sms
importlib.reload(sms)


def get_performers(m_start, m_end, master_df):
    '''
        Calculating return within period of trading hours
        Note: Working dataset starts from 9:30 minute and ends 15:59.
        Takes:
            - m_start (int): minute to start (if from very begining of day - should be 0 - 9:30)
            - m_end (int): minute to end (eg. 5 will end at X5 minute inclusive - 9:35)
            - master_df (DataFrame): intraday minute data for all tickers; columns:
            ['open', 'high', 'low', 'close', 'volume', 'ticker']
                
        Examples:
        [-1:0] - returns previous day close (15:59) and 9:30 data close minute calculated
        [-1:1] - returns previous day close through 9:31 data close minute calculated
        [1:2] - returns 9:31 to 9:32 minutes calculated
        etc.
    '''
    # initializing minutes
    if m_start == -1:
        tm_index_start = datetime.datetime(1999, 1, 1, 15, 59, 0).time()
    else:  
        tm_index_start = (datetime.datetime(1999, 1, 1, 9, 30, 0) + timedelta(minutes=m_start)).time()
    tm_index_end = (datetime.datetime(1999, 1, 1, 9, 30, 0) + timedelta(minutes=m_end)).time()
    
    # creating empty dataframe
    cmp_m = pd.DataFrame(index = [tm_index_end])

    # iterating through tickers and making statistical dataframe
    for s in master_df.ticker.unique():
        try:
            d = master_df.loc[master_df['ticker'] == s] # GETTING stock data from db
            returns = []
            # appending returns for each date and calculating mean (incluides previous day closing price)
            p_dy = None
            for dy in sorted(np.unique(d.index.date), reverse=False):
                # if previous day involved
                if m_start == -1:
                    # verifying previous date
                    if len(np.unique(d.index.date)) < 2:
                        print("Error: get_performers - No previous day for "+\
                              "{} identified as requested".format(s))
                        return None
                    # if not from begining of dataset - previous day closing price required
                    if p_dy != None:
                        # getting price of closed 15.59 minute of previous day
                        prev_d_close_price = \
                            d.loc[d.index == \
                                datetime.datetime(p_dy.year,p_dy.month,p_dy.day,
                                                  15,59,0)]['close'].values[0]
                        # getting price of specified closed minute of current day
                        cur_d_m_price = \
                            d.loc[d.index == \
                                datetime.datetime(dy.year,dy.month,dy.day,
                                                  tm_index_end.hour,
                                                  tm_index_end.minute,0)]['close'].values[0]
                        # calculating and appending return
                        ret = u.price_change(float(prev_d_close_price), float(cur_d_m_price))
                        returns.append(ret)
                else:
                    # getting price of first requested closed minute
                    prev_m_close_price = \
                        d.loc[d.index == \
                            datetime.datetime(dy.year,dy.month,dy.day,
                                              tm_index_start.hour,
                                              tm_index_start.minute,0)]['close'].values[0]
                    # getting price of last requested closed minute
                    cur_m_close_price = \
                        d.loc[d.index == \
                            datetime.datetime(dy.year,dy.month,dy.day,
                                              tm_index_end.hour,
                                              tm_index_end.minute,0)]['close'].values[0]
                    # calculating and appending return
                    ret = u.price_change(float(prev_m_close_price), float(cur_m_close_price))
                    returns.append(ret)
                p_dy = dy
            # appending dataframe with data for each ticker
            ndf = pd.DataFrame([np.mean(returns)], index = [tm_index_end], columns=[s])
            cmp_m = cmp_m.join(ndf)
        except Exception as ex:
            print("Error: get_performers - Failed to get {}: {}".format(s, ex))
            pass
    return cmp_m


def get_volumers(m_start, m_end, master_df):
    '''
        Calculating mean of all volumes within period of trading hours.
        Note: Working dataset starts from 9:30 minute and ends 15:59.
        Takes:
            - m_start (int): minute to start (if from very begining of day - should be 0 - 9:30)
            - m_end (int): minute to end (eg. 5 will end at X5 minute inclusive - 9:35)
            - master_df (DataFrame): intraday minute data for all tickers; columns:
            ['open', 'high', 'low', 'close', 'volume', 'ticker']
                
        Examples:
        [-1:0] - returns previous day close (15:59) and 9:30 data close minute calculated
        [-1:1] - returns previous day close through 9:31 data close minute calculated
        [1:2] - returns 9:31 to 9:32 minutes calculated
        etc.
    '''
    # initializing minutes
    if m_start == -1:
        tm_index_start = datetime.datetime(1999, 1, 1, 15, 59, 0).time()
    else:  
        tm_index_start = (datetime.datetime(1999, 1, 1, 9, 30, 0) + timedelta(minutes=m_start)).time()
    tm_index_end = (datetime.datetime(1999, 1, 1, 9, 30, 0) + timedelta(minutes=m_end)).time()
    
    # creating empty dataframe
    cmp_m = pd.DataFrame(index = [tm_index_end])
    
    # iterating through tickers and making statistical dataframe
    for s in master_df.ticker.unique():
        try:
            d = master_df.loc[master_df['ticker'] == s] # GETTING stock data from db
            volumes = pd.Series(np.nan)
            # appending volumes for each date and calculating mean (incluides previous day closing volume)
            p_dy = None
            for dy in sorted(np.unique(d.index.date), reverse=False):
                # if previous day involved
                if m_start == -1:
                    # verifying previous date
                    if len(np.unique(d.index.date)) < 2:
                        print("Error: get_volumers - No previous day for "+\
                              "{} identified as requested".format(s))
                        return None
                    # if not from begining of dataset - previous day closing price required
                    if p_dy != None:
                        # getting volume of 16.00 minute of previous day (in series)
                        prev_d_sr = \
                            d.loc[d.index == \
                                datetime.datetime(p_dy.year,p_dy.month,p_dy.day,
                                                  15,59,0)]['volume']
                        volumes = volumes.append(prev_d_sr)
                        # getting series of volumes of current day
                        cur_day_dt = d.loc[d.index.date == dy].copy()
                        cur_d_sr = cur_day_dt.loc[(cur_day_dt.index.time >= \
                                        datetime.datetime(1999,1,1,9,30).time())&\
                                       (cur_day_dt.index.time <= \
                                        tm_index_end)]['volume']
                        # concatenating series
                        volumes = volumes.append(cur_d_sr)
                else:
                    # getting series of volumes of current day and calculating mean
                    cur_day_dt = d.loc[d.index.date == dy].copy()
                    cur_d_sr_s = cur_day_dt.loc[(cur_day_dt.index.time >= \
                                    tm_index_start)&\
                                   (cur_day_dt.index.time <= \
                                    tm_index_end)]['volume']
                    # appending to list
                    volumes = volumes.append(cur_d_sr_s)
                p_dy = dy
            # calculating meand and appending to dataframe with data for each ticker
            ndf = pd.DataFrame([int(np.mean(volumes))], index = [tm_index_end], columns=[s])
            cmp_m = cmp_m.join(ndf)
        except Exception as ex:
            print("Error: get_volumers - Failed to get {}: {}".format(s, ex))
            pass
    return cmp_m

# stats function to get return for current day
def curday_minute_ret_stock_idx(data, ticker, prev_mnt, cur_mnt, day):
    '''
        returns specific minute after specific minute return for given stock
        
        takes:
            data - master dataframe pandas
            ticker - ticker
            prev_mnt - number of first minute (read get_performers() description, consider not using previous day here)
            cur_mnt - number of last minute (read get_performers() description, consider not using previous day here)
            
            !!! ATTENTION !!!: this function DOES NOT USE Previous day.
            
            Examples:
            [0:2] - returns 9:30 to 9:32 data calculated
            [1:3] - returns 9:31 and 9:33 data calculated
                
            day - datetime.date (pd.Timestamp(xxxx, x, x, x, x).date()) 
    '''
    try:
        cur_day = data.loc[(data.index.date == day) & (data.ticker == ticker)].copy()
        cur_day.iloc[0] # check availability of data
    except Exception as ex:
        print("Error: curday_minute_ret_stock_idx - Failed to get data for "+\
              "{} at {}: {}".format(ticker, day, ex))
        return None
    # verifying integrity
    if cur_day.loc[cur_day.index.time == pd.Timestamp(2000, 1, 1, 9, 30, 0).time()].empty:
        print("Error: curday_minute_ret_stock_idx - No 9:30 minute data for "+\
              "{} at {}".format(ticker, day))
        return None

    # defining shapes/interval minutes
    ms = prev_mnt
    me = cur_mnt
                      
    try:
        return get_performers(ms, me, cur_day).iloc[0][0]
    except Exception as ex:
        print("Error: curday_minute_ret_stock_idx - Failed to get returns for "+\
              "{} at {}: {}".format(ticker, day, ex))
        return None  
    
# stats function to get averages
def cur_avg_stats_stock_idx(func, data, ticker, interval, day=None):
    '''
        returns average return or average volume for given stock (inlcudes previous
                                                                 closing day record)
            for 'interval' number of minutes (measured after 9:30)
        takes:
            func - "return", "volume"
            data - master dataframe pandas
            ticker - ticker
            interval - minute interval to get average on
            
            e.g.
            interval = 5: prev_d 15.59 - cur_d 9.35
            
            read get_performers() function for data structure reference
            
            !!!Attention!!! Function TAKES preceeding day close price
                        to put as starting minute for calculation.
            
            day - datetime.date (pd.Timestamp(xxxx, x, x, x, x).date()) or None.
                                    If None - all dates picked in dataframe.
    '''
    # specific date
    if day != None:
        
        # making list of dates
        all_dates = [dt for dt in sorted(np.unique(data.index.date), reverse=False)]
        
        # preparing preceeding day
        try:
            pre_day_idx = all_dates.index(day)-1
            if pre_day_idx < 0:
                print("Error: cur_avg_stats_stock_idx - There is no previous day for", day)
                return None
            ind_df = data.loc[(data.index.date == all_dates[pre_day_idx]) & (data.ticker == ticker)].copy()
            ind_df.iloc[0] # check availability of data
        except Exception as ex:
            print("Error: cur_avg_stats_stock_idx - Failed to get data for preceeding day "+\
                  "{} at {}: {}".format(ticker,
                                                                          all_dates[pre_day_idx],
                                                                          ex))
            return None
        # verifying integrity
        if ind_df.loc[ind_df.index.time == pd.Timestamp(2000, 1, 1, 15, 59, 0).time()].empty:
            print("Error: cur_avg_stats_stock_idx - No 15:59 minute data for "+\
                  "{} at {}".format(ticker, all_dates[pre_day_idx]))
            return None
        
        # appending current day
        try:
            cur_day = data.loc[(data.index.date == day) & (data.ticker == ticker)].copy()
            cur_day.iloc[0] # check availability of data
            ind_df = ind_df.append(cur_day)
        except Exception as ex:
            print("Error: cur_avg_stats_stock_idx - Failed to get data for "+\
                  "{} at {}: {}".format(ticker, day, ex))
            return None
        # verifying integrity
        if cur_day.loc[cur_day.index.time == pd.Timestamp(2000, 1, 1, 9, 30, 0).time()].empty:
            print("Error: cur_avg_stats_stock_idx - No 9:30 minute data for "+\
                  "{} at {}".format(ticker, day))
            return None
        
    # overall frame
    else:
        try:
            ind_df = data.loc[data.ticker == ticker].copy()
            ind_df.iloc[0] # check availability of data
        except Exception as ex:
            print("Error: cur_avg_stats_stock_idx - Failed to get data for "+\
                  "{}: {}. Skipping ticker...".format(ticker, day, ex))
            return None
        # verifying integrity of dataframe for each day to have equal size
        new_ind_df = pd.DataFrame(columns = ind_df.columns)
        for dt in sorted(np.unique(ind_df.index.date), reverse=False):
            not_to_trim_df = ind_df.loc[ind_df.index.date == \
                                        pd.Timestamp(dt).date()].copy()
            if not_to_trim_df.loc[not_to_trim_df.index.time == \
                                  pd.Timestamp(2000, 1, 1, 9, 30, 0).time()].empty:
                print("Error: cur_avg_stats_stock_idx - No 9:30 minute data for "+\
                      "{} at {}. Continue processing without this day...".format(ticker, dt))
                pass
            else:
                new_ind_df = new_ind_df.append(not_to_trim_df)
        # renaming frame
        ind_df = new_ind_df.copy()
    
    # defining shapes/interval minutes
    me = interval
    ms = -1
                      
    # returns calculation
    if func == "return":
        try:
            return get_performers(ms, me, ind_df).iloc[0][0]
        except Exception as ex:
            print("Error: cur_avg_stats_stock_idx - Failed to get returns for "+\
                  "{} at {}: {}".format(ticker, day, ex))
            return None
    
    # volume calculations
    elif func == "volume":
        try:
            return get_volumers(ms, me, ind_df).iloc[0][0]
        except Exception as ex:
            print("Error: cur_avg_stats_stock_idx - Failed to get volumes for "+\
                  "{} at {}: {}".format(ticker, day, ex))
            return None
    else:
        print("Error: cur_avg_stats_stock_idx - wrong 'func' argument. "+\
              "Should be 'return' or 'volume' instead '{}'".format(func))
        return None    
 
# stats function to get averages
def prev_avg_stats_stock_idx(func, data, ticker, interval, day=None, trim_mins=181):
    '''
        *****
        READ CAREFULLY BEFORE USAGE !!!
        *****
        
        returns average return or average volume for given stock
            for 'interval' number of minutes before market close (15:59 minute)
        takes:
            func - "return", "volume"
            data - master dataframe pandas
            ticker - ticker
            interval - minute interval to get average on
            day - EXACT day to calculate datetime.date (pd.Timestamp(xxxx, x, x, x, x).date()) or None.
                                    If None - all dates picked in dataframe EXCEPT last one that is not
                                        used for previous closed days
            trim_mins - minutes to trimm day to before closure (for processing speed)
    '''
    # specific date
    if day != None:
        try:
            ind_df = data.loc[(data.index.date == day) & (data.ticker == ticker)].copy()
            ind_df.iloc[0] # check availability of data
        except Exception as ex:
            print("Error: prev_avg_stats_stock_idx - Failed to get data for "+\
                  "{} at {}: {}".format(ticker, day, ex))
            return None
        # verifying integrity
        if ind_df.loc[ind_df.index.time == pd.Timestamp(2000, 1, 1, 15, 59, 0).time()].empty:
            print("Error: prev_avg_stats_stock_idx - No 15:59 minute data for "+\
                  "{} at {}".format(ticker, day))
            return None
        # defining shapes/interval minutes
        me = 389 # 15:59 minute
        ms = me - interval
    
    # overall frame
    else:
        try:
            ind_df = data.loc[data.ticker == ticker].copy()
            ind_df.iloc[0] # check availability of data
        except Exception as ex:
            print("Error: prev_avg_stats_stock_idx - Failed to get data for "+\
                  "{} date: {}. Skipping ticker...".format(ticker, day, ex))
            return None
        # verifying integrity and trimming dataframe for each day to have equal size
        new_ind_df = pd.DataFrame(columns = ind_df.columns)
        # excluding current/last day to calculate average for array of previous days
        for dt in [i for i in sorted(np.unique(ind_df.index.date), reverse=False)][:-1]:
            to_trim_df = ind_df.loc[ind_df.index.date == \
                                    pd.Timestamp(dt).date()].copy()
            if to_trim_df.loc[to_trim_df.index.time == \
                              pd.Timestamp(2000, 1, 1, 15, 59, 0).time()].empty:
                #print("Error: No 16:00 minute data for "+\
                #"{} at {}. Continue processing without this date...".format(ticker, dt))
                #return None
                pass
            else:
                # trimming to speed up processing
                new_ind_df = new_ind_df.append(to_trim_df[-trim_mins:])
            
        # renaming frame
        ind_df = new_ind_df.copy()
        # defining shapes/interval minutes
        me = 389 # 15:59 minute
        ms = me - interval        
                      
    # returns calculation
    if func == "return":
        try:
            return get_performers(ms, me, ind_df).iloc[0][0]
        except Exception as ex:
            print("Error: prev_avg_stats_stock_idx Failed to get returns for "+\
                  "{} at {}: {}".format(ticker, day, ex))
            return None
    
    # volume calculations
    elif func == "volume":
        try:
            return get_volumers(ms, me, ind_df).iloc[0][0]
        except Exception as ex:
            print("Error: Failed to get volumes for "+\
                  "{} at {}: {}".format(ticker, day, ex))
            return None
    else:
        print("Error: wrong 'func' argument. Should be 'return' or 'volume' "+\
              "instead '{}'".format(func))
        return None
    
# get open high low close day price
def ohcl_day(data, ticker, day):
    '''
        returns ohcl data for ticker at given day
        takes:
            data - master dataframe pandas
            ticker - ticker
            day - datetime.date (pd.Timestamp(xxxx, x, x, x, x).date())
    '''
    # taking specific date
    try:
        ind_df = data.loc[(data.index.date == day) & (data.ticker == ticker)].copy()
    except Exception as ex:
        print("Error: ohcl_day - Failed to get data for "+\
              "{} at {}: {}".format(ticker, day, ex))
        return None
    
    # getting metrics
    try:
        opn = ind_df.loc[ind_df.index.time == \
                         datetime.datetime(1999,1,1,9,30).time()]['open'].values[0]
        cle = ind_df.loc[ind_df.index.time == \
                         datetime.datetime(1999,1,1,15,59).time()]['close'].values[0]
        hgh = ind_df.loc[(ind_df.index.time >= \
                          datetime.datetime(1999,1,1,9,30).time())&\
                         (ind_df.index.time <= \
                          datetime.datetime(1999,1,1,15,59).time())]['high'].max()
        low = ind_df.loc[(ind_df.index.time >= \
                          datetime.datetime(1999,1,1,9,30).time())&\
                         (ind_df.index.time <= \
                          datetime.datetime(1999,1,1,15,59).time())]['low'].min()
        return opn, hgh, cle, low
    except Exception as ex:
        print("Error: ohcl_day - Failed to fetch OHCL data for "+\
              "{} at {}: {}".format(ticker, day, ex))
        return None

# get day average volume
def avg_vol_day(data, ticker, day):
    '''
        returns volume average data for ticker at given day
        takes:
            data - master dataframe pandas
            ticker - ticker
            day - datetime.date (pd.Timestamp(xxxx, x, x, x, x).date())
    '''
    # taking specific date
    try:
        ind_df = data.loc[(data.index.date == day) & (data.ticker == ticker)].copy()
        return int(ind_df.loc[(ind_df.index.time >= \
                          datetime.datetime(1999,1,1,9,30).time())&\
                         (ind_df.index.time <= \
                          datetime.datetime(1999,1,1,15,59).time())]['volume'].mean())
    except Exception as ex:
        print("Error: avg_vol_day - Failed to get data for {} at {}: {}".format(ticker, day, ex))
        return None
    
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 9

market data collection

@author: vyachez
"""
# Imports
import numpy as np
import pandas as pd
from tqdm import tqdm
import random
import time
from os import sys
import more_itertools
import logging

import importlib
import strack_utils as u
importlib.reload(u)
import strack_sql as stkl
importlib.reload(stkl)
import strack_trade_exec as trader
importlib.reload(trader)
import strack_delivery as sms
importlib.reload(sms)

def get_stock(tickers, day, wait=5, db_path=None,
              scope = "full",
              strict=False,
              attempts = 10, acc_type=None):
    '''
        parses intraday STOCK minute data from Polygon (with identical Yahoo Finance backup)
         and UPDATES intraday table in database.
        THIS IS 1 DAY COLLECTION ONLY function with 5 days going backwards MAX.
        takes:
        - tickers - list - all tickers of stocks
        - day - datetime.datetime(yyyy,m,d).date() to collect data for
        - wait - int - time to wait in seconds before uploading next ticker.
        - db_path - str - path to database sqlite3.
        - scope - str - "full" or "compact" if "compact" (limits to 60 minutes output)
        - strict - bool - in case if check for data integrity needed for each ticker
        - atempts - int  - to collect ticker in case of error (need to be more than 5 to change data provider)
        - acc_type - "paper" or "market"
    
    Returns:
            Pandas Dataframe - loaded tickers data in strict predefined format:
                columns = ['open', 'high', 'low', 'close', 'volume', 'ticker']
                index - datetime.datetime()
                index.name = "time"
            OR
            None if saved to db.
        
        ATTENTION: DATA LOSS or CORRUPTION may occur if dates picked wrong.
        
    '''    
    provider = "polygon"
    def_wait = 10 # default wait time
    
    if attempts < 6:
        msg = "Attention: if less "+\
            "than 6 attempts - backup collection "+\
                "method/provider will not be triggered in case of fail."
        print(msg)
        logging.warning(msg)   
    
    msg = "Collecting data from "+\
        "{}: '{}' scope, strict mode - '{}'.".format(str(provider).upper(),
                                                     scope,
                                                     strict)
    print(msg)
    logging.info(msg)
    
    # creating blank dataframe
    master_df = pd.DataFrame(columns = ['open', 'high', 'low', 'close',
                                        'volume', 'ticker'])
            
    # defaulting variables
    base_provider = provider
    base_attempts = attempts
    # iterating through tickers and uploading
    for s in tqdm(tickers):
        ticker_empty = False
        if db_path != None:
            time.sleep(def_wait)
        else:
            time.sleep(wait)
        if strict:
            try:
                no_data = True
                wait_att = def_wait # time to wait if fail (incremental)
                at_count = 0
                attempts = base_attempts
                provider = base_provider
                while no_data:
                    # selecting provider method
                    if provider == "yfinance":
                        d = u.get_ticker_yfinance(s)
                    elif provider == "polygon":
                        # verifying account
                        if acc_type == None:
                            logging.critical("Fatal: Specify account type")
                            sys.exit()
                        # getting day data
                        start_date = end_date = str(day)
                        # limiting scope
                        if scope == "compact":
                            limit = 60 # 1 hour
                        else:
                            limit = 0
                        d = u.get_ticker_polygon(s, acc_type, 1,
                            start_date, end_date, limit)
                    else:
                        logging.critical("Fatal: "+\
                                         "wrong provider name {}".format(provider))
                        sys.exit()
                    # verifying integrity
                    if d.empty != True:
                        no_data = False
                        if d.loc[d.index.date == day].shape[0] == 0:
                            no_data = True
                    if no_data:
                        msg = "Error: Got BLANK {} data for {}. Retrying...".format(day, s)
                        print(msg)
                        logging.warning(msg)
                        attempts = attempts - 1
                        at_count += 1
                        time.sleep(wait_att)
                        wait_att += 5
                        # smart change of data provider
                        if at_count > 4 and provider != "yfinance":
                            provider = "yfinance"
                            wait_att = 5
                            msg = "Trying changing data provider "+\
                                "to {} to get {} data".format(provider, s)
                            print(msg)
                            logging.warning(msg)
                    if attempts == 0:
                        ticker_empty = True
                        msg = "I could not get data for {}.".format(s)
                        print(msg)
                        logging.critical(msg)
                        no_data = False 
            except Exception as ex:
                msg = "Unexpected Error: unable to get and convert data for {}: {}".format(s, ex)
                print(msg)
                logging.warning(msg)
                time.sleep(def_wait+5)
                continue
        else:
            try:
                # selecting provider method
                if provider == "yfinance":
                    d = u.get_ticker_yfinance(s)
                elif provider == "polygon":
                    # verifying account
                    if acc_type == None:
                        logging.critical("Fatal: Specify account type")
                        sys.exit()
                    # getting day data
                    start_date = end_date = str(day)
                    # limiting scope
                    if scope == "compact":
                        limit = 60 # 1 hour
                    else:
                        limit = 0
                    d = u.get_ticker_polygon(s, acc_type, 1,
                        start_date, end_date, limit)
                else:
                    logging.critical("Fatal: "+\
                                     "wrong provider name {}".format(provider))
                    sys.exit()
                # verifying integrity
                if d.empty != True:
                    if d.loc[d.index.date == day].shape[0] == 0:
                        ticker_empty = True
                        msg = "Got BLANK {} data for {}.".format(day, s)
                        print(msg)
                        logging.warning(msg)
                else:
                    ticker_empty = True
                    msg = "Got BLANK {} data for {}.".format(day, s)
                    print(msg)
                    logging.warning(msg)
            except Exception as ex:
                msg = "Unexpected Error: unable to get and convert data for {}: {}".format(s, ex)
                print(msg)
                logging.warning(msg)
                time.sleep(def_wait+5)
                continue

        if ticker_empty != True:
            try:
                d = d.loc[d.index.date == day].copy()
                master_df = master_df.append(d, ignore_index = False, sort=False)
            except Exception as ex:
                msg = "Error: unable to add ticker data {} to master: {}".format(s, ex)
                print(msg)
                logging.warning(msg)
                time.sleep(def_wait+5)
        else:
            msg = "No {} ticker data".format(s)
            print(msg)
            logging.warning(msg)
    if master_df.empty:
        msg = "No data collected!"
        print(msg)
        logging.warning(msg)
        return None
    else:
        master_df = master_df.sort_index()
        master_df.index.name = "time"
        if db_path != None:
            # saving collected data to database - intraday table
            stkl.rec_db_intraday_df(db_path, master_df)
            msg = "Saved collected data for {} to intraday table.".format(day)
            print(msg)
            logging.info(msg)
        else:
            return master_df   
    
def get_stock_bulk(ticker,
                      start_date,
                      end_date,
                      attempts = 3,
                      acc_type = None,
                      db_path = None):
    '''
        Designed to get stock for wide time frames for single ticker.
        POLYGON provider ONLY.
        
        Does not verify each date.
        
        If you need day to day data use get_stock() function.
        
        takes:
            - ticker - str - ticker
            - start_date, end_date - datetime.datetime(yyyy,mm,dd).date() - range
            - atempts - int  - to collect ticker in case of error
            - acc_type - "paper" or "market"
            - db_path - str - path to database sqlite3.
        Returns:
            Pandas Dataframe - loaded tickers data in strict predefined format:
                columns = ['open', 'high', 'low', 'close', 'volume', 'ticker']
                index - datetime.datetime()
                index.name = "time" 
    '''
    # verifying account
    if acc_type == None:
        msg = "Fatal: Specify account type"
        logging.critical(msg)
        sys.exit()
    
    # message
    msg = "Bulk data collection from POLYGON for "+\
        "{} from {} to {}".format(ticker, start_date, end_date)
    print(msg)
    logging.info(msg)

    def_wait = 10 # default wait time

    ticker_empty = False
    # getting data
    try:
        no_data = True
        wait_att = def_wait # time to wait if fail (incremental)
        at_count = 0
        while no_data:
            d = u.get_ticker_polygon(ticker, acc_type, 1,
                start_date, end_date, 0)
            # verifying integrity
            if d.empty != True:
                no_data = False
            if no_data:
                msg = "Error: Got BLANK data for {}. Retrying...".format(ticker)
                print(msg)
                logging.warning(msg)
                attempts = attempts - 1
                at_count += 1
                time.sleep(wait_att)
                wait_att += 5
            if attempts == 0:
                msg = "I could not get data for {}.".format(ticker)
                ticker_empty = True
                print(msg)
                logging.critical(msg)
                no_data = False 
    except Exception as ex:
        msg = "Unexpected Error: unable to get and convert data for {}: {}".format(ticker, ex)
        ticker_empty = True
        print(msg)
        logging.warning(msg)
        time.sleep(def_wait+5)

    if ticker_empty == True:
        msg = "No {} ticker data".format(ticker)
        print(msg)
        logging.warning(msg)
    else:
        d = d.sort_index()
        if db_path != None:
            # saving collected data to database - intraday table
            stkl.rec_db_intraday_df(db_path, d)
            msg = "Saved collected data for {}.".format(ticker)
            print(msg)
            logging.info(msg)
        else:
            return d 
        
def batch_tickers_collector(tickers, start_date, end_date, acc_type, path,
                            db_path, deliver=False):
    '''Collects wide range dates by batches for provided multiple tickers
        All args are mandatory - explanation at get_stock_bulk function
        
        tickers - list
        Start_date, End_date - datetime.datetime() only (no date() at the end)
        
        Workflow: breaks requested dates by batch of 'batch_counter' days (maximum capacity
        for single upload via Polygon) and downloads for each ticker saving to database
        
        CAUTION! DATA corruption may occur if used inaccurately
    '''
    def err_logger(err_id, log, ticker, date, error):
        '''
            recording errors
        '''
        log[err_id] = {'err_id':err_id,
                       'ticker':ticker,
                       'date':date,
                       'error':error}
        return log
    
    # account
    tx = trader.Trader(acc_type=acc_type, deliver=False,
                                   stop_limit=1)
    
    # getting tradable days
    days_trade = tx.api.get_calendar(start_date, end_date)
    
    msg_notif = "Batch Downloader for tickers data activated."
    logging.info(msg_notif)
    print(msg_notif)
    if deliver:
        sms.send(msg_notif)
    
    # batching days by downloadable days sizer
    batch_counter = 5
    days_batched = list(more_itertools.windowed(days_trade,n=batch_counter, step=batch_counter))
    for batch in days_batched:
        print("*************")
        st_d = list(filter(None, batch))[0].date.date()
        en_d = list(filter(None, batch))[-1].date.date()
        print("Batched dates: {} - {}:".format(st_d, en_d))
        for d in list(filter(None, batch)):
            print(d.date.date())
            
    # downloading by batches with start date and end date
    count = 0
    err_log = {}
    print("Started downloading...\n")
    try:
        for batch in tqdm(days_batched):
            print("*************")
            st_d = list(filter(None, batch))[0].date.date()
            en_d = list(filter(None, batch))[-1].date.date()
            # downloading tickers
            for tk in tqdm(tickers):
                get_stock_bulk(tk, st_d, en_d,
                                  attempts = 3,
                                  acc_type = acc_type,
                                  db_path = db_path)
                # checking if download is successful
                print("Checking...")
                master = stkl.get_db_intraday_ticker(db_path, tk) # obtaining database
                master_dates = np.array(np.unique(master.index.date))
                # iterating via dataframe to check if all dates downloaded
                for d in list(filter(None, batch)):
                    if d.date.date() not in master_dates:
                        msg = "ERROR: {} date was not downloaded for {}".format(d.date.date(), tk)
                        err_log = err_logger(count, err_log, tk, d.date.date(), msg)
                        print(msg)
                        logging.warning(msg)
                        if deliver:
                            sms.send(msg) 
                        count+=1
                sl = random.randint(5, 33)
                print("Sleeping {} seconds".format(sl))
                time.sleep(sl)
            sl = random.randint(5, 120)
            print("Sleeping {} seconds before next batch".format(sl))
            time.sleep(sl)
    
        # final dates check
        print("Final Checking for dates...")
        master = stkl.get_db_intraday_all(db_path) # obtaining database
        master_dates = np.array(np.unique(master.index.date))
        # iterating via dataframe to check if all dates downloaded
        for d in list(filter(None, days_trade)):
            if d.date.date() not in master_dates:
                msg = "ERROR: {} date was not downloaded for {}".format(d.date.date(), tk)
                err_log = err_logger(count, err_log, tk, d.date.date(), msg)
                print(msg)
                logging.warning(msg)
                count+=1
    
        msg = "Finished bulk collection"
        print(msg)
        logging.info(msg)
        if deliver:
            sms.send(msg)
            
    except Exception as ex:
        msg = "Unexpected error: {}. Terminating".format(ex)
        print(msg)
        logging.critical(msg)
        if deliver:
            sms.send(msg)
        err_log = err_logger(count+1, err_log, None, None, msg)
        # recording error log
        columns=['err_id','ticker','date','error']
        err_log_file = pd.DataFrame(columns=columns) 
        try:
            # opening log ledger
            filename = path+'/strack_data/tick_batch_load_log.csv'
            for idx in err_log:
                tmp_frame = pd.DataFrame([[
                    err_log[idx]['err_id'],
                    err_log[idx]['ticker'],
                    err_log[idx]['date'],
                    err_log[idx]['error']]],
                    columns=columns)
                err_log_file = err_log_file.append(tmp_frame,
                                    ignore_index=True,
                                    sort=False)
            # recording to file
            err_log_file.to_csv(filename, index=False)
            msg = "successfully recorded log"
            print(msg)
            logging.warning(msg)
            sys.exit()
        except Exception as ex:
            msg = "ERROR: Failed to record error log: "+\
            "{}".format(ex)
            print(msg)
            logging.warning(msg)
            sys.exit()
    
    # Successfull load        
    # recording error log
    columns=['err_id','ticker','date','error']
    err_log_file = pd.DataFrame(columns=columns) 
    try:
        # opening log ledger
        filename = path+'/strack_data/tick_batch_load_log.csv'
        for idx in err_log:
            tmp_frame = pd.DataFrame([[
                err_log[idx]['err_id'],
                err_log[idx]['ticker'],
                err_log[idx]['date'],
                err_log[idx]['error']]],
                columns=columns)
            err_log_file = err_log_file.append(tmp_frame,
                                ignore_index=True,
                                sort=False)
    
        # recording to file
        err_log_file.to_csv(filename, index=False)
        msg = "successfully recorded log"
        print(msg)
        logging.warning(msg)
    except Exception as ex:
        msg = "ERROR: Failed to record error log: "+\
        "{}".format(ex)
        print(msg)
        logging.warning(msg)
        
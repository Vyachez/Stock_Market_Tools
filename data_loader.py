#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 9

Stock data loader

@author: vyachez
"""
import numpy as np
import pandas as pd
import requests as r
import json
import datetime

import importlib
import strack_trader_pl as apis
importlib.reload(apis)

def get_ticker_polygon(ticker, acc_type, multiplier,
                       start_date, end_date, limit):
    '''
        Getting intraday ticker data with Polygon.
        DOES NOT HAVE Everything: STOCKS only.
        Takes:
            - ticker - str - equity
            - acc_type - str - trading account type ("paper" or "market")
            - multiplier - int - Size of the timespan multiplier (minutes)
            - start_date, end_date - str - dates of query range, eg. "2020-01-01"
            - limit - int - number of last minutes to return (0 - all)
        Returns:
            Pandas Dataframe - loaded tickers data in strict predefined format:
                columns = ['open', 'high', 'low', 'close', 'volume', 'ticker']
                index - datetime.datetime()
                index.name = "time"
    '''
    # Polygon API
    api_code = apis.get_api(acc_type, credentials=True)
    # unique path to Polygon source 
    url_source = "https://api.polygon.io/v2/aggs/ticker/"+\
                "{ticker}/range/".format(ticker=ticker)+\
                "{multiplier}/".format(multiplier=multiplier)+\
                "{timespan}/".format(timespan="minute")+\
                "{}/{}".format(start_date,end_date)+\
                "?apiKey={}".format(api_code)
    obj = r.get(url_source)
    jsn = json.loads(obj.text)
    if len(jsn) == 0:
        tck_d = pd.DataFrame(columns=['empty'])
        print("No Polygon source data for {}".format(ticker))
        return tck_d
    # pipeline
    cols = ['open', 'high', 'low', 'close', 'volume']
    tck_d = pd.DataFrame(columns = cols)
    # loop
    try:
        for row in jsn['results'][-limit:]:
            tmp = pd.DataFrame([row])
            tmp_r = tmp[['o','h','l','c','v']].copy()
            tmp_r.columns = cols
            tmp_r['time'] = pd.Timestamp(datetime.datetime.fromtimestamp(int(tmp['t'][0]) / 1000))
            tck_d = tck_d.append(tmp_r, sort=False)
        tck_d.index = tck_d['time']
        tck_d.drop(labels=['time'], axis=1, inplace=True)
        tck_d['ticker'] = ticker
    except Exception as ex:
        print("Error getting data from Polygon for {}: {}".format(ticker, ex))
        tck_d = pd.DataFrame(columns=cols)
    if tck_d.empty:
        print("No Polygon source data for {}: {}".format(ticker, jsn))
    return tck_d

def get_tradable_tickers_polygon(market='stocks', page=1):
    '''returns df with tickers supported by Polygon
    takes:
    market - str - type of market:
        indices
        stocks
        crypto
        fx
        bonds
        mf
        mmf
    page - int - page to return (API does not support all pages)
    '''
    # Polygon API
    acc_type = "paper"
    api_code = apis.get_api(acc_type, credentials=True)
    # unique path to Polygon source 
    url_source = "https://api.polygon.io/v2/reference/tickers"+\
        "?sort=ticker&type=cs&market={}".format(market)+\
            "&locale=us"+\
            "&perpage=50"+\
            "&page={}".format(page)+\
            "&apiKey={}".format(api_code)
    obj = r.get(url_source)
    tickers_all_polygon = json.loads(obj.text)
    # creating dataframe with all available tickers from Polygon
    cols = ['ticker','name','market','locale','currency','active','primaryExch','updated','url']
    polygon_tickers = pd.DataFrame(columns = cols)
    # loop
    for row in tickers_all_polygon['tickers']:
        tmp = pd.DataFrame([row])
        tmp_r = tmp[['ticker','name','market','locale','currency','active','primaryExch','updated','url']].copy()
        tmp_r.columns = cols
        polygon_tickers = polygon_tickers.append(tmp_r, sort=False)
    polygon_tickers.reset_index(inplace=True, drop=True)
    return polygon_tickers

def get_last_trade_polygon(ticker, acc_type):
    '''
        Getting latest ticker traded price with Polygon
        DOES NOT HAVE Everything: STOCKS only.
    '''
    # Polygon API
    api_code = apis.get_api(acc_type, credentials=True)
    # unique path to Polygon source 
    url_source = "https://api.polygon.io/v1/last/stocks/"+\
                "{}?apiKey={}".format(ticker,api_code)
    obj = r.get(url_source)
    jsn = json.loads(obj.text)
    price = float(jsn['last']['price'])
    return price

def get_chg_price(ticker, minute, acc_type):
    '''
        Checking on price change using historical data
        between 'minute' (int) closed minute and
        latest closed minute data on provided 'ticker'
        POLYGON based only
        DOES NOT HAVE Everything: STOCKS only.
    '''
    date = str(datetime.datetime.now().date())
    d = get_ticker_polygon(ticker=ticker, acc_type=acc_type,
                           multiplier=1, start_date=date,
                           end_date=date, limit=minute*2)
    cur_price = float(d.iloc[-1]['4. close'])
    prev_price = float(d.iloc[-minute]['4. close'])
    chg = (cur_price-prev_price)/prev_price
    return prev_price, cur_price, chg
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 9

Stock data loader

@author: vyachez
"""
# Imports
import pandas as pd
import requests as r
import json

# AlphaVantage API
api_code = " "

def get_ticker(ticker, vol='compact'):
    # unique path to AlphaVantage source 
    url_source = "https://www.alphavantage.co/query?function="+\
                "TIME_SERIES_INTRADAY&symbol={}&interval=".format(ticker)+\
                "1min&outputsize={}&apikey={}".format(vol,api_code)
    obj = r.get(url_source)
    jsn = json.loads(obj.text)
    tck_d = pd.DataFrame.from_dict(jsn["Time Series (1min)"], orient='index')
    return tck_d

def price_change(old, new):
    return (new-old)/old
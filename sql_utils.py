#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  8 19:16:12 2020
@author: vyachez

STRACK SQL functions
"""

import sqlite3
from sqlite3 import Error

import pandas as pd

def create_db(db_path):
    """ create SQLite database
    takes:
        - db_path - string - database desired path and name """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print("Ver {}. Created db".format(sqlite3.version))
    except Error as e:
        print("Error: {}".format(e))
    finally:
        if conn:
            conn.close()

def connect_db(db_path):
    """ create a database connection to a SQLite database
        takes:
            - db_path - string - database path"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print("Ver {}. Connected to db".format(sqlite3.version))
    except Error as e:
        print("Error: {}".format(e))
    return conn

def create_intraday_table(db_path):
    """ create a table for intraday data
    takes:
        - db_path - string - database path"""
    # table variables
    create_table_sql = "CREATE TABLE IF NOT EXISTS intraday "+\
                                "(time text,"+\
                                    "open real,"+\
                                    "high real,"+\
                                    "low real,"+\
                                    "close real,"+\
                                    "volume real,"+\
                                    "ticker text)"
    # creating connection
    conn = connect_db(db_path)
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
        print("Created intraday table successfully")
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def get_tables(db_path):
    '''returns list of tables in provided database
    takes:
        - db_path - string - database path'''
    # creating connection
    conn = connect_db(db_path)
    # creating cursor
    c = conn.cursor()
    # getting tables
    tables = c.execute('SELECT name from sqlite_master where type= "table"')
    tab_list = tables.fetchall()
    # closing connection
    conn.close()
    return tab_list

def rec_db_intraday_df(db_path, df):
    '''
    Records tickers dataframe to intraday table database.
    
    Parameters
    ----------
    db_path : string - database path
    df : Pandas Dataframe - loaded tickers data in strict predefined format:
        columns = ['open', 'high', 'low', 'close', 'volume', 'ticker']
        index - datetime.datetime()
        index.name = "time"
    
    Returns
    -------
    None.
    '''
    # creating connection
    conn = connect_db(db_path)
    # recording df to database
    df.to_sql("intraday", con=conn, if_exists='append', index_label='time')
    conn.commit()
    # closing connection
    conn.close()
    print("Successfully recorded data to intraday table")
    
def get_db_intraday_all(db_path):
    ''' returning all data from intraday db table as dataframe
        filtered and sorted for common application.
        takes:
            - db_path - string - database path'''
    # creating connection
    conn = connect_db(db_path)
    # getting dataframe
    df = pd.read_sql(sql='SELECT * FROM intraday', con=conn,
                     index_col=['time'],
                     parse_dates=['time']).sort_index()
    # closing connection
    conn.close()
    print("Successfully retrieved all data from intraday table")
    return df

def get_db_intraday_date(db_path, date):
    ''' returning data filtered by date from intraday db table as dataframe
        filtered and sorted for common application.
        takes:
            - db_path - string - database path
            - date - datetime.datetime(yyy, m, d).date() object'''
    # creating connection
    conn = connect_db(db_path)
    # selecting by criteria
    sel = 'SELECT * from intraday WHERE time '+\
        'LIKE "{}%"'.format(date)
    # getting dataframe
    df = pd.read_sql(sql=sel, con=conn,
                     index_col=['time'],
                     parse_dates=['time']).sort_index()
    # closing connection
    conn.close()
    print("Successfully retrieved requested data from intraday table")
    return df

def get_db_intraday_ticker(db_path, ticker):
    ''' returning data filtered by date from intraday db table as dataframe
        filtered and sorted for common application.
        takes:
            - db_path - string - database path
            - ticker - string - ticker'''
    # creating connection
    conn = connect_db(db_path)
    # selecting by criteria
    sel = 'SELECT * from intraday WHERE ticker '+\
        '= "{}"'.format(ticker)
    # getting dataframe
    df = pd.read_sql(sql=sel, con=conn,
                     index_col=['time'],
                     parse_dates=['time']).sort_index()
    # closing connection
    conn.close()
    print("Successfully retrieved requested data from intraday table")
    return df

def get_db_intraday_date_ticker(db_path, ticker, date):
    ''' returning data filtered by date and ticker from intraday db table as dataframe
        filtered and sorted for common application.
        takes:
            - db_path - string - database path
            - ticker - string - ticker
            - date - datetime.datetime(yyy, m, d).date() object'''
    # creating connection
    conn = connect_db(db_path)
    # selecting by criteria
    sel = 'SELECT * from intraday WHERE ticker '+\
        '= "{}" AND time LIKE "{}%"'.format(ticker, date)
    # getting dataframe
    df = pd.read_sql(sql=sel, con=conn,
                     index_col=['time'],
                     parse_dates=['time']).sort_index()
    # closing connection
    conn.close()
    print("Successfully retrieved requested data from intraday table")
    return df

def delete_db_intraday_date(db_path, date):
    '''Deleting data for required date from intraday table database
    takes:
        - db_path - string - database path
        - date - datetime.datetime(yyy, m, d).date() object
        '''
    # creating connection
    conn = connect_db(db_path)
    # creating cursor
    c = conn.cursor()
    # deleting by criteria
    delete = 'DELETE from intraday WHERE '+\
        'time LIKE "{}%"'.format(date)
    c.execute(delete)
    conn.commit()
    print("Deleted {} from intraday table successfully.".format(date))

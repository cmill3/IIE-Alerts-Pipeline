import pandas as pd
import requests 
import json
from datetime import timedelta, datetime
from helpers import data
import boto3
import logging
from botocore.exceptions import ClientError
import concurrent.futures
import math
import numpy as np
import pandas_market_calendars as mcal

api_key = 'XpqF6xBLLrj6WALk4SS1UlkgphXmHQec'

big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","FB","CRM"
            ]
sf = ['TSLA','NIO','RIVN','XPEV','COIN','ROKU','LCID',
         'WBD','SQ','SNAP','ZM','SHOP','DOCU','ROKU','TWLO','PINS','SNAP','UBER','LYFT','DDOG',
         'ZS','NET','CMG','ARM','OKTA','UPST','ETSY','AXP','TDOC','PINS','NCLH','UAL','AAL','DAL',
         'FUTU','SE','BILI','BIDU','JD','BABA','MMM','PEP','GE','CCL','RCL','MRK','RBLX','COIN',
         'HD','LOW','AFFRM','VZ','T','PG','TSM']
new_bf = ['C','CAT','KO','MS','GS','PANW','ORCL','IBM','CSCO','WMT','TGT','COST']
indexes = ['QQQ','SPY','IWM']

sf = ['GME','AMC','MARA','TSLA','BBY','NIO','RIVN','XPEV','COIN','ROKU','LCID',
         'WBD','SQ','SNAP','ZM','SHOP','DOCU','ROKU','TWLO','PINS','SNAP','UBER','LYFT','DDOG',
         'ZS','NET','CMG','ARM','OKTA','UPST','ETSY','AXP','TDOC','PINS','NCLH','UAL','AAL','DAL',
         'FUTU','SE','BILI','BIDU','JD','BABA','MMM','PEP','GE','CCL','RCL','MRK','RBLX','COIN',
         'HD','LOW','AFFRM','VZ','T','PG','TSM','NKE','SBUX']
new_bf = ['C','CAT','KO','MS','GS','PANW','ORCL','IBM','CSCO','WMT','TGT','COST']

bf_plus = ["AMD","NVDA","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP","FB","CRM",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","NKE",'META',
            'C','TGT','MMM','SQ','PANW','DAL','CSCO','UBER','SBUX','QQQ','SPY','IWM','TLT']

all_symbols = ['ZM', 'UBER', 'CMG', 'AXP', 'TDOC', 'UAL', 'DAL', 'MMM', 'PEP', 'GE', 'RCL', 'MRK',
 'HD', 'LOW', 'VZ', 'PG', 'TSM', 'GOOG', 'GOOGL', 'AMZN', 'BAC', 'AAPL', 'ABNB',
 'CRM', 'MSFT', 'F', 'V', 'MA', 'JNJ', 'DIS', 'JPM', 'ADBE', 'BA', 'CVX', 'PFE',
 'META', 'C', 'CAT', 'KO', 'MS', 'GS', 'IBM', 'CSCO', 'WMT','TSLA','LCID','NIO','WFC',
 'TGT', 'COST', 'RIVN', 'COIN', 'SQ', 'SHOP', 'DOCU', 'ROKU', 'TWLO', 'DDOG', 'ZS', 'NET',
 'OKTA', 'UPST', 'ETSY', 'PINS', 'FUTU', 'SE', 'BIDU', 'JD', 'BABA', 'RBLX', 'AMD',
 'NVDA', 'PYPL', 'PLTR', 'NFLX', 'CRWD', 'INTC', 'MRNA', 'SNOW', 'SOFI', 'PANW',
 'ORCL','SBUX','NKE','FB']

nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

s3 = boto3.client('s3')

def options_snapshot_remediator(date_str):
    for symbol in ['PLTR','F','SNOW','BAC']:
        print(symbol)
        try:
            dt_str = date_str.replace('-','/')
            res = s3.get_object(Bucket='icarus-research-data', Key=f'options_snapshot/{dt_str}/{symbol}.csv')
            continue
        except Exception as e:
            try:
                monday = previous_monday(date_str)
                fridays = find_fridays(monday)
                call_tickers, put_tickers = build_options_tickers(symbol, fridays, monday)
                call_df = get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
            except Exception as e:
                print(f"This symbol: {symbol} failed twice {e}")
                continue
    return "done"

def options_snapshot_runner(monday):
    print(monday)
    fridays = find_fridays(monday)
    date_str = monday.replace('-','/')
    for symbol in bf_plus:
        print(symbol)
        try:
            res = s3.get_object(Bucket='icarus-research-data', Key=f'options_snapshot/{date_str}/{symbol}.csv')
            continue
        except Exception as e:
            try:
                call_tickers, put_tickers = build_options_tickers(symbol, fridays, monday)
                call_df = get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
            except Exception as e:
                print(f"This symbol: {symbol} failed twice {e}")
                continue
    return "done"

def get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol):
    hours = ["10","11","12","13","14","15"]
    timedelta_to_add = [0,1,2,3,4]
    dt = datetime.strptime(monday, "%Y-%m-%d")
    monday_np = np.datetime64(monday)
    if monday_np in holidays_multiyear:
        timedelta_to_add  = [1,2,3,4]
    
    for day in timedelta_to_add:
        date = dt + timedelta(days=day)
        date_stamp = date.strftime("%Y-%m-%d")
        print(date_stamp)
        call_df = data.call_polygon_backtest(call_tickers,from_stamp=date_stamp,to_stamp=date_stamp,timespan="day",multiplier="1")
        put_df = data.call_polygon_backtest(put_tickers,from_stamp=date_stamp,to_stamp=date_stamp,timespan="day",multiplier="1")
        call_df['option_type'] = 'call'
        put_df['option_type'] = 'put'
        final_df = pd.concat([call_df,put_df],ignore_index=True)
        csv = final_df.to_csv()
        date_str = date.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
        key_str = date_str.replace('-','/')
        put_response = s3.put_object(Bucket='icarus-research-data', Key=f'options_snapshot/{key_str}/{symbol}.csv', Body=csv)

def build_strikes(monday,ticker):
    last_price = data.call_polygon_price_day(ticker,from_stamp=monday,to_stamp=monday,timespan="day",multiplier="1")
    price_floor = math.floor(last_price *.2)
    price_ceil = math.ceil(last_price *1.2)
    strikes = np.arange(price_floor, price_ceil, .5)
    return strikes

def build_options_tickers(symbol, fridays, monday):
    call_tickers = []
    put_tickers = []
    monday_np = np.datetime64(monday)
    if monday_np in holidays_multiyear:
        monday_dt = pd.to_datetime(monday_np)
        tuesday_dt = monday_dt + timedelta(days=1)
        tuesday = tuesday_dt.strftime("%Y-%m-%d")
        print(tuesday)
        strikes = build_strikes(tuesday,symbol)
    else:
        strikes = build_strikes(monday,symbol)

    for strike in strikes:
        for friday in fridays:
            call_tickers.append(build_option_symbol(symbol,friday,strike,"call"))
            put_tickers.append(build_option_symbol(symbol,friday,strike,"put"))
    return call_tickers, put_tickers

def build_option_symbol(ticker, date, strike, option_type):
    #Extract the year, month, and day from the date
    date = date.strftime("%Y-%m-%d")
    year, month, day = date.split('-')
    short_year = year[-2:]
    str_strk = str(strike)

    # Convert the strike price
    if '.5' in str_strk:
        str_strk = str_strk.split('.')[0]
        if len(str_strk) == 4:
            strike_formatted = f"0{str_strk}500"
        elif len(str_strk) == 3:
            strike_formatted = f"00{str_strk}500"
        elif len(str_strk) == 2:
            strike_formatted = f"000{str_strk}500"
        elif len(str_strk) == 1:
            strike_formatted = f"0000{str_strk}500"
        elif len(str_strk) == 1:
            strike_formatted = f"00000{str_strk}500"
    else:
        str_strk = str_strk.split('.')[0]
        if len(str_strk) == 4:
            strike_formatted = f"0{str_strk}000"
        elif len(str_strk) == 3:
            strike_formatted = f"00{str_strk}000"
        elif len(str_strk) == 2:
            strike_formatted = f"000{str_strk}000"
        elif len(str_strk) == 1:
            strike_formatted = f"0000{str_strk}000"


    # Determine the option type character
    if option_type.lower() == 'call':
        type_char = 'C'
    elif option_type.lower() == 'put':
        type_char = 'P'
    else:
        raise ValueError("Invalid option type. Must be 'call' or 'put'.")

    return f"O:{ticker}{short_year}{month}{day}{type_char}{strike_formatted}"

def find_fridays(monday):
    monday = datetime.strptime(monday, "%Y-%m-%d")
    # Check if the provided datetime is a Monday
    if monday.weekday() != 0:
        raise ValueError("Provided datetime is not a Monday")
    
    # Calculate the next three Fridays
    first_friday = monday + timedelta(days=4)  # 4 days from Monday
    second_friday = first_friday + timedelta(days=7)
    third_friday = second_friday + timedelta(days=7)
    
    return [first_friday, second_friday, third_friday]



def previous_monday(date_str):
    # Convert the input string to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Calculate the number of days to subtract to get to the previous Monday
    # .weekday() returns 0 for Monday, 1 for Tuesday, ..., 6 for Sunday
    days_to_subtract = (date_obj.weekday() - 0) % 7

    # If the given day is Monday, days_to_subtract will be 0. 
    # To get the previous Monday, we need to subtract 7 days in this case.
    if days_to_subtract == 0:
        days_to_subtract = 7

    # Subtract the calculated number of days
    previous_monday = date_obj - timedelta(days=days_to_subtract)

    return previous_monday.strftime("%Y-%m-%d")



if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2022,10,1)
    end_date = datetime(2023,11,18)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        ## Change this to affect if it just adds mondays or all weekdays
        if temp_date.weekday() == 0:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)
        


    # options_snapshot_runner("2023-01-02")
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(options_snapshot_runner, date_str) for date_str in date_list]
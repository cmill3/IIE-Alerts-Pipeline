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
import os

api_key = 'XpqF6xBLLrj6WALk4SS1UlkgphXmHQec'

big_fish =  ["AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","FB","CRM"]

all_symbols = ['ZM', 'UBER', 'CMG', 'AXP', 'TDOC', 'UAL', 'DAL', 'MMM', 'PEP', 'GE', 'RCL', 'MRK',
 'HD', 'LOW', 'VZ', 'PG', 'TSM', 'GOOG', 'GOOGL', 'AMZN', 'BAC', 'AAPL', 'ABNB',
 'CRM', 'MSFT', 'F', 'V', 'MA', 'JNJ', 'DIS', 'JPM', 'ADBE', 'BA', 'CVX', 'PFE',
 'META', 'C', 'CAT', 'KO', 'MS', 'GS', 'IBM', 'CSCO', 'WMT','TSLA','LCID','NIO','WFC',
 'TGT', 'COST', 'RIVN', 'COIN', 'SQ', 'SHOP', 'DOCU', 'ROKU', 'TWLO', 'DDOG', 'ZS', 'NET',
 'OKTA', 'UPST', 'ETSY', 'PINS', 'FUTU', 'SE', 'BIDU', 'JD', 'BABA', 'RBLX', 'AMD',
 'NVDA', 'PYPL', 'PLTR', 'NFLX', 'CRWD', 'INTC', 'MRNA', 'SNOW', 'SOFI', 'PANW',
 'ORCL','SBUX','NKE','FB']


bfpidx = ["AMD","NVDA","PYPL","GOOG","GOOGL","AMZN","BAC","AAPL","FB","DIS"
          "MSFT","INTC","PFE","SNOW",'META','C','XOM',"QQQ","SPY","IWM","TLT"]

remaining = ["GOOG","GOOGL","AMZN","BAC","FB"
            "MSFT",'META',"TLT"]

indexes = ["QQQ","SPY","IWM"]

nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

s3 = boto3.client('s3', aws_access_key_id="AKIAWUN5YYJZHGIGMLQJ", aws_secret_access_key="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")

def options_snapshot_runner(monday,symbol):
    fridays = find_fridays(monday)
    try:
        print(symbol)
        call_tickers, put_tickers = build_options_tickers(symbol, fridays, monday)
        get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
        print(f"Finished {monday} for {symbol}")
    except Exception as e:
        print(f"{symbol} failed at {monday} with: {e}. Retrying")
        try:
            call_tickers, put_tickers = build_options_tickers(symbol, fridays, monday)
            get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
            print(f"Finished {monday} for {symbol}")
        except Exception as e:
            print(f"{symbol} failed twice at {monday} with: {e}. Skipping")
    

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
        for hour in hours:
            call_df = data.call_polygon_PCR(call_tickers,from_stamp=date_stamp,to_stamp=date_stamp,timespan="hour",multiplier="1",hour=hour)
            put_df = data.call_polygon_PCR(put_tickers,from_stamp=date_stamp,to_stamp=date_stamp,timespan="hour",multiplier="1",hour=hour)
            call_df['option_type'] = 'call'
            put_df['option_type'] = 'put'
            final_df = pd.concat([call_df,put_df],ignore_index=True)
            csv = final_df.to_csv()
            date_str = date.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
            key_str = date_str.replace('-','/')
            put_response = s3.put_object(Bucket='icarus-research-data', Key=f'options_snapshot/{key_str}/{hour}/{symbol}.csv', Body=csv)

def build_strikes(monday,ticker):
    last_price = data.call_polygon_price_day(ticker,from_stamp=monday,to_stamp=monday,timespan="day",multiplier="1")
    price_floor = math.floor(last_price *.75)
    price_ceil = math.ceil(last_price * 1.25)
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



if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2018,1,1)
    end_date = datetime(2023,10,28)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() == 0:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)


    for symbol in ["AMZN","TLT"]:
        print(f"Starting {symbol}")
        cpu_count = (os.cpu_count()*2)
        print(cpu_count)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count) as executor:
            # Submit the processing tasks to the ThreadPoolExecutor
            processed_weeks_futures = [executor.submit(options_snapshot_runner,date_str,symbol) for date_str in date_list]
        print(f"Finished {symbol}")

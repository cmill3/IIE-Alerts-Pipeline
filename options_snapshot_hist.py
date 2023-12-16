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
 'HD', 'LOW', 'VZ', 'PG', 'TSM', 'FB', 'SOFI', 'PANW',
 'GOOG', 'GOOGL', 'AMZN', 'BAC', 'AAPL','DIS','META', 'C', 'MSFT','PFE',
 'ABNB','CRM', 'F', 'V', 'MA', 'JNJ', 'JPM', 'ADBE', 'BA', 'CVX',
 'CAT', 'KO', 'MS', 'GS', 'IBM', 'CSCO','TSLA','LCID','NIO','WFC',
 'TGT', 'COST', 'RIVN', 'COIN', 'SQ', 'SHOP', 'DOCU', 'ROKU', 'TWLO', 'DDOG', 'ZS', 'NET',
 'OKTA', 'UPST', 'ETSY', 'PINS', 'FUTU', 'SE', 'BIDU', 'JD', 'BABA', 'RBLX', 
 'AMD','NVDA', 'PYPL', 'PLTR', 'NFLX', 'CRWD', 'INTC', 'MRNA', 'SNOW','XOM'
 'ORCL','WBD','ARM','SNAP','BILI','AAL','CCL','NCLH','LYFT','BIDU','JD','BABA','HD','LOW',
 'SBUX','NKE','AFFRM','WMT','QCOM','AVGO','TXN','MU','AMAT','CVNA','DKNG','MGM','CZR','RCLH']

first_run = ['ZM', 'UBER', 'CMG', 'AXP', 'TDOC', 'UAL', 'DAL', 'MMM', 'PEP', 'GE', 'RCL', 'MRK',
 'HD', 'LOW', 'VZ', 'PG', 'TSM', 'FB'
#  'GOOG', 'GOOGL', 'AMZN', 'BAC', 'AAPL','DIS','META', 'C', 'MSFT','PFE',
 'ABNB','CRM', 'F', 'V', 'MA', 'JNJ', 'JPM', 'ADBE', 'BA', 'CVX',
  'CAT', 'KO', 'MS', 'GS', 'IBM', 'CSCO','TSLA','LCID','NIO','WFC',
 'TGT', 'COST', 'RIVN', 'COIN', 'SQ', 'SHOP', 'DOCU', 'ROKU', 'TWLO', 'DDOG', 'ZS', 'NET',
 'OKTA', 'UPST', 'ETSY', 'PINS', 'FUTU', 'SE', 'BIDU', 'JD', 'BABA', 'RBLX', 
#  'AMD','NVDA', 'PYPL', 'PLTR', 'NFLX', 'CRWD', 'INTC', 'MRNA', 'SNOW',XOM
 'SOFI', 'PANW',
 'ORCL','WBD','ARM','SNAP','BILI','AAL','CCL','NCLH','LYFT','BIDU','JD','BABA','HD','LOW',
 'SBUX','NKE','AFFRM','WMT','QCOM','AVGO','TXN','MU','AMAT','CVNA','DKNG','MGM','CZR','RCLH']


bfpidx = ["AMD","NVDA","PYPL","GOOG","GOOGL","AMZN","BAC","AAPL","FB","DIS"
          "MSFT","INTC","PFE","SNOW",'META','C','XOM',"QQQ","SPY","IWM","TLT"]

remaining = ["AMZN","BAC","FB","DIS"
            "MSFT",'META',"TLT"]

indexes = ["QQQ","SPY","IWM"]

nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

s3 = boto3.client('s3', aws_access_key_id="AKIAWUN5YYJZHGIGMLQJ", aws_secret_access_key="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")

def options_snapshot_runner(monday,symbol):
     ### Implementation for index options
    # fridays = find_fridays(monday)
    date_str = monday.replace('-','/')
    ## for symbol in ['SPY','IWM']: This is for you Dean.
    days = build_days(symbol, monday)
    try:
        print(symbol)
        call_tickers, put_tickers = build_options_tickers(symbol, days, monday)
        get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
        print(f"Finished {monday} for {symbol}")
    except Exception as e:
        print(f"{symbol} failed at {monday} with: {e}. Retrying")
        try:
            call_tickers, put_tickers = build_options_tickers(symbol, days, monday)
            get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)
            print(f"Finished {monday} for {symbol}")
        except Exception as e:
            print(f"{symbol} failed twice at {monday} with: {e}. Skipping")

def options_snapshot_remediator(date_str,symbol):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hours = ["10","11","12","13","14","15"]
    date_np = np.datetime64(dt)
    if date_np in holidays_multiyear:
        return "holiday"
    else:
        for hour in hours:
            try:
                dt_str = date_str.replace('-','/')
                res = s3.get_object(Bucket='icarus-research-data', Key=f'options_snapshot/{dt_str}/{hour}/{symbol}.csv')
            except Exception as e:
                print(f"{symbol} had {e} at {date_str}")
                try:
                    monday = previous_monday(date_str)
                    fridays = find_fridays(monday)
                    call_tickers, put_tickers = build_options_tickers_remediate(symbol, fridays, monday, date_str)
                    call_df = get_options_snapshot_hist_remediate(call_tickers, put_tickers, monday, symbol, hour, date_str)
                except Exception as e:
                    print(f"This symbol: {symbol} failed twice {e}")
        print(f"Finished {date_str} for {symbol}")
        return "done"
        
def options_snapshot_remediator_idx(date_str,symbol):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hours = ["10","11","12","13","14","15"]
    date_np = np.datetime64(dt)
    if date_np in holidays_multiyear:
        return "holiday"
    else:
        for hour in hours:
            # try:
            #     dt_str = date_str.replace('-','/')
            #     res = s3.get_object(Bucket='icarus-research-data', Key=f'options_snapshot/{dt_str}/{hour}/{symbol}.csv')
            # except Exception as e:
            #     print(f"{symbol} had {e} at {date_str}")
                # try:
                monday = previous_monday(date_str)
                days = build_days_remdiator(monday)
                call_tickers, put_tickers = build_options_tickers_remediate(symbol, days, monday, date_str)
                call_df = get_options_snapshot_hist_remediate(call_tickers, put_tickers, monday, symbol, hour, date_str)
                # except Exception as e:
                #     print(f"This symbol: {symbol} failed twice {e}")
        print(f"Finished {date_str} for {symbol}")
        return "done"
    

def get_options_snapshot_hist_remediate(call_tickers, put_tickers, monday, symbol, hour, date_str):
    call_df = data.call_polygon_PCR(call_tickers,from_stamp=date_str,to_stamp=date_str,timespan="hour",multiplier="1",hour=hour)
    put_df = data.call_polygon_PCR(put_tickers,from_stamp=date_str,to_stamp=date_str,timespan="hour",multiplier="1",hour=hour)
    call_df['option_type'] = 'call'
    put_df['option_type'] = 'put'
    final_df = pd.concat([call_df,put_df],ignore_index=True)
    key_str = date_str.replace('-','/')
    put_response = s3.put_object(Bucket='icarus-research-data', Key=f'options_snapshot/{key_str}/{hour}/{symbol}.csv', Body=final_df.to_csv())

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
    price_floor = math.floor(last_price *.8)
    price_ceil = math.ceil(last_price * 1.2)
    strikes = np.arange(price_floor, price_ceil, 1)
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

def build_options_tickers_remediate(symbol, fridays, monday, date_str):
    call_tickers = []
    put_tickers = []
    strikes = build_strikes(date_str,symbol)
    for strike in strikes:
        for friday in fridays:
            call_tickers.append(build_option_symbol(symbol,friday,strike,"call"))
            put_tickers.append(build_option_symbol(symbol,friday,strike,"put"))
    return call_tickers, put_tickers

def build_option_symbol(ticker, date, strike, option_type):
    #Extract the year, month, and day from the date
    if type(date) != str:
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

def build_days(symbol, monday):
    opt_dates = []
    # if symbol == 'IWM':
    to_add = [0,2,4,7,9,11]
    # else:
    #     to_add = [0,1,2,3,4,7,8,9,10,11]
    
    for x in to_add:
        dt = datetime.strptime(monday, "%Y-%m-%d")
        date = dt + timedelta(days=x)
        date_str = date.strftime("%Y-%m-%d")
        opt_dates.append(date_str)
    return opt_dates



if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2018,1,1)
    end_date = datetime(2023,11,18)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)


    for symbol in ["IWM"]:
        print(f"Starting {symbol}")
        cpu_count = (os.cpu_count()*2)
        # options_snapshot_runner(date_list[0],symbol)

        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count) as executor:
            # Submit the processing tasks to the ThreadPoolExecutor
            processed_weeks_futures = [executor.submit(options_snapshot_runner,date_str,symbol) for date_str in date_list]
        print(f"Finished {symbol}")

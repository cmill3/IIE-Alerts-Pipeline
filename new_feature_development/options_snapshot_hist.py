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

api_key = 'XpqF6xBLLrj6WALk4SS1UlkgphXmHQec'

big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","FB","CRM"
            ]
indexes = ['QQQ','SPY','IWM']

s3 = boto3.client('s3')

def options_snapshot_runner(monday):
    # dates = build_dates(monday)
    fridays = find_fridays(monday)
    for symbol in big_fish:
        call_tickers, put_tickers = build_options_tickers(symbol, fridays)
        call_df = get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol)

def get_options_snapshot_hist(call_tickers, put_tickers, monday, symbol):
    hours = ["10","11","12","13","14","15"]
    timedelta_to_add = [0,1,2,3,4]
    for day in timedelta_to_add:
        date = monday + timedelta(days=day)
        date_stamp = datetime.strptime(date, "%Y-%m-%d")
        for hour in hours:
            call_df = data.call_polygon_PCR(call_tickers,from_stamp=date_stamp,to_stamp=date_stamp,timespan="hour",multiplier="1",hour=hour)
            put_df = data.call_polygon_PCR(put_tickers,from_stamp=date_stamp,to_stamp=date_stamp,timespan="hour",multiplier="1",hour=hour)
            call_df['option_type'] = 'call'
            put_df['option_type'] = 'put'
            final_df = pd.concat([call_df,put_df],ignore_index=True)
            put_response = s3.put_object(Bucket='icarus-reaearch-data', Key=f'options_snapshot/{date}/{hour}/{symbol}.csv', Body=final_df.to_csv(index=False))

def build_strikes(monday,ticker):
    last_price = data.call_polygon_price_day(ticker,from_stamp=monday,to_stamp=monday,timespan="day",multiplier="1")
    price_floor = math.floor(last_price *.5)
    price_ceil = math.ceil(last_price *1.5)
    return list(range(price_floor,price_ceil,.5))

def build_options_tickers(symbol, fridays, monday):
    call_tickers = []
    put_tickers = []
    strikes = build_strikes(monday,symbol)
    for strike in strikes:
        for friday in fridays:
            call_tickers.append(build_option_symbol(symbol,friday,strike,"call"))
            put_tickers.append(build_option_symbol(symbol,friday,strike,"put"))
    return call_tickers, put_tickers

def build_option_symbol(ticker, date, strike, option_type):
    #Extract the year, month, and day from the date
    year, month, day = date.split('-')
    short_year = year[-2:]

    # Convert the strike price
    strike_formatted = "00000" + str(int(strike * 100)).zfill(5)

    # Determine the option type character
    if option_type.lower() == 'call':
        type_char = 'C'
    elif option_type.lower() == 'put':
        type_char = 'P'
    else:
        raise ValueError("Invalid option type. Must be 'call' or 'put'.")

    return f"{ticker}{short_year}{month}{day}{type_char}{strike_formatted}"

def find_fridays(monday):
    monday = datetime.strptime(monday, "%Y-%m-%d")
    # Check if the provided datetime is a Monday
    if monday.weekday() != 0:
        raise ValueError("Provided datetime is not a Monday")
    
    # Calculate the next three Fridays
    first_friday = monday + timedelta(days=4)  # 4 days from Monday
    second_friday = first_friday + timedelta(days=7)
    third_friday = second_friday + timedelta(days=7)
    
    return [first_friday.strf, second_friday, third_friday]



if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2020,8,21)
    end_date = datetime(2023,9,23)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() == 0:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(options_snapshot_runner, date_str) for date_str in date_list]
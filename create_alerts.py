import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import *
from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError
import concurrent.futures
import warnings
warnings.filterwarnings("ignore")

alerts_bucket = os.getenv("ALERTS_BUCKET")
all_symbols = ['ZM', 'UBER', 'CMG', 'AXP', 'TDOC', 'UAL', 'DAL', 'MMM', 'PEP', 'GE', 'RCL', 'MRK',
 'HD', 'LOW', 'VZ', 'PG', 'TSM', 'GOOG', 'GOOGL', 'AMZN', 'BAC', 'AAPL', 'ABNB',
 'CRM', 'MSFT', 'F', 'V', 'MA', 'JNJ', 'DIS', 'JPM', 'ADBE', 'BA', 'CVX', 'PFE',
 'META', 'C', 'CAT', 'KO', 'MS', 'GS', 'IBM', 'CSCO', 'WMT','TSLA','LCID','NIO','WFC',
 'TGT', 'COST', 'RIVN', 'COIN', 'SQ', 'SHOP', 'DOCU', 'ROKU', 'TWLO', 'DDOG', 'ZS', 'NET',
 'OKTA', 'UPST', 'ETSY', 'PINS', 'FUTU', 'SE', 'BIDU', 'JD', 'BABA', 'RBLX', 'AMD',
 'NVDA', 'PYPL', 'PLTR', 'NFLX', 'CRWD', 'INTC', 'MRNA', 'SNOW', 'SOFI', 'PANW',
 'ORCL','SBUX','NKE','FB']

def run_process(date_str):
    try:
        print(date_str)
        alerts_runner(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        alerts_runner(date_str)
    print(f"Finished {date_str}")

def alerts_runner(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        aggregates, error_list = call_polygon_histD(all_symbols, from_stamp, to_stamp, timespan="minute", multiplier="30")
        hour_aggregates, error_list = call_polygon_histH(all_symbols, hour_stamp, hour_stamp, timespan="minute", multiplier="30")
        full_aggs = combine_hour_aggs(aggregates, hour_aggregates, hour)
        alerts = build_alerts(full_aggs)
        for alert in alerts:
            csv = alerts[alert].to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/{key_str}/{alert}/{hour}.csv", Body=csv)

def add_data_to_alerts(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        for type in ['gainers','losers']:
            alert = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/{key_str}/{type}/{hour}.csv")
            sf = s3.get_object(Bucket="inv-alerts", Key=f"sf/vol/{key_str}/{hour}.csv")
            sf= pd.read_csv(sf['Body'])
            bf = s3.get_object(Bucket="inv-alerts", Key=f"bf/vol/{key_str}/{hour}.csv")
            bf= pd.read_csv(bf['Body'])
            alert = pd.read_csv(alert['Body'])
            full_data = pd.concat([sf,bf])
            full_df = full_data.loc[full_data['symbol'].isin(alert['symbol'])]
            alert = alert[['symbol','hour']]
            alerts_data = alert.merge(full_df, on=['symbol','hour'])
            alerts_data = alerts_data.drop_duplicates(subset=['symbol'])
            s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/data/{key_str}/{type}/{hour}.csv", Body=alerts_data.to_csv())
    print(f"Finished with {date_str}")
        

def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    for index, value in enumerate(aggregates):
        hour_aggs = hour_aggregates[index]
        hour_aggs = hour_aggs.loc[hour_aggs["hour"] < int(hour)]
        if len(hour_aggs) > 1:
            hour_aggs = hour_aggs.iloc[:-1]
        volume = hour_aggs.v.sum()
        open = hour_aggs.o.iloc[0]
        close = hour_aggs.c.iloc[-1]
        high = hour_aggs.h.max()
        low = hour_aggs.l.min()
        n = hour_aggs.n.sum()
        t = hour_aggs.t.iloc[-1]
        aggs_list = [volume, open, close, high, low, hour_aggs.date.iloc[-1], hour,hour_aggs.symbol.iloc[-1],t]
        value.loc[len(value)] = aggs_list
        value['close_diff'] = value['c'].pct_change().round(4)
        full_aggs.append(value)
    return full_aggs

def build_alerts(df):
    df = pd.concat(df)
    alerts = df.groupby("symbol").tail(1)
    c_sorted = alerts.sort_values(by="close_diff", ascending=False)
    gainers = c_sorted.head(30)
    losers = c_sorted.tail(30)
    return {"gainers": gainers, "losers": losers}

def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=8)
    # end_day = end - timedelta(days=1)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp


if __name__ == "__main__":
    # build_historic_data(None, None)
    print(os.cpu_count())
    start_date = datetime(2018,1,1)
    end_date = datetime(2023,10,28)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    # # for date_str in date_list:
    # add_data_to_alerts("2022-01-27")
        

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
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
# from helpers.constants import ALL_SYM, TRADING_SYMBOLS, WEEKLY_EXP
warnings.filterwarnings("ignore")

alerts_bucket = os.getenv("ALERTS_BUCKET")

vol_features = ['return_vol_15M', 'volume_vol_15M', 'return_vol_30M', 'volume_vol_30M', 'return_vol_60M', 
'volume_vol_60M', '15min_vol_diff', '15min_vol_diff_pct', 'min_vol_diff', 
'min_vol_diff_pct', 'min_volume_vol_diff', 'min_volume_vol_diff_pct', 'return_vol_4H', 'return_vol_8H', 
'return_vol_16H', 'volume_vol_4H', 'volume_vol_8H', 'volume_vol_16H', 'hour_vol_diff', 'hour_vol_diff_pct', 
'hour_volume_vol_diff', 'hour_volume_vol_diff_pct', 'return_vol_3D', 'return_vol_5D', 'return_vol_10D', 
'return_vol_30D', 'volume_vol_3D', 'volume_vol_5D', 'volume_vol_10D', 'volume_vol_30D', 'daily_vol_diff', 
'daily_vol_diff_pct', 'daily_vol_diff30', 'daily_vol_diff_pct30', 'daily_volume_vol_diff', 'daily_volume_vol_diff_pct', 
'daily_volume_vol_diff30', 'daily_volume_vol_diff_pct30','symbol']


now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3')
logger = logging.getLogger()


def run_process(date_str):
    try:
        print(f"Starting {date_str}")
        build_vol_features(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        build_vol_features(date_str)
    print(f"Finished {date_str}")

def build_vol_features(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    from_stamp, to_stamp, hour_stamp = generate_dates_historic_vol(date_str)

    for hour in hours:
        df = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/{key_str}/{hour}.csv")
        df = pd.read_csv(df['Body'])
        # df = df.loc[df['symbol'].isin(['ORCL'])]
        symbols = df['symbol'].unique().tolist()
        min_aggs, error_list = call_polygon_vol(symbols, from_stamp, to_stamp, timespan="minute", multiplier="1", hour=hour)
        hour_aggs, error_list = call_polygon_vol(symbols, from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
        results_df = vol_feature_engineering(df, min_aggs, hour_aggs)
        # old_df = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/vol/{key_str}/{hour}.csv")
        # old_df = pd.read_csv(old_df['Body'])
        # new_df = pd.concat([old_df,results_df],ignore_index=True)
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/vol/{key_str}/{hour}.csv", Body=results_df.to_csv())
    return put_response


def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    for index, value in enumerate(aggregates):
        hour_aggs = hour_aggregates[index]
        hour_aggs = hour_aggs.loc[hour_aggs["hour"] <= int(hour)]
        hour_aggs = hour_aggs.iloc[:-1]
        volume = hour_aggs.v.sum()
        open = hour_aggs.o.iloc[0]
        close = hour_aggs.c.iloc[-1]
        high = hour_aggs.h.max()
        low = hour_aggs.l.min()
        n = hour_aggs.n.sum()
        t = hour_aggs.t.iloc[-1]
        # hour_dict = {"v": volume, "vw":0, "o":open, "c":close, "h":high, "l":low, "t":t,"n":n,"date":hour_aggs.date.iloc[-1],"hour":hour,"minute":0,"symbol":hour_aggs.symbol.iloc[-1]}
        aggs_list = [volume, open, close, high, low, hour_aggs.date.iloc[-1], hour,hour_aggs.symbol.iloc[-1],t]
        value.loc[len(value)] = aggs_list
        full_aggs.append(value)
    return full_aggs


def generate_dates_historic_vol(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=8)
    # end_day = end - timedelta(days=1)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

def remediate_alerts(date_str):
    from_stamp, to_stamp, hour_stamp = generate_dates_historic_vol(date_str)
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    hours = ["10","11","12","13","14","15"]
    for hour in hours:
        try:
            df = s3.get_object(Bucket="inv-alerts", Key=f"sf/vol/{key_str}/{hour}.csv")
        except Exception as e:
            print(f"{e} for {key_str} {hour}")
            df = s3.get_object(Bucket="inv-alerts", Key=f"sf/{key_str}/{hour}.csv")
            df = pd.read_csv(df['Body'])
            min_aggs, error_list = call_polygon_vol(df['symbol'], from_stamp, to_stamp, timespan="minute", multiplier="1", hour=hour)
            hour_aggs, error_list = call_polygon_vol(df['symbol'], from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
            results_df = vol_feature_engineering(df, min_aggs, hour_aggs)
            csv = results_df.to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"sf/vol/{key_str}/{hour}.csv", Body=csv)
            print(f"Finished {key_str} {hour}")
    return put_response

def consolidate_bf_vol(date_str):
    from_stamp, to_stamp, hour_stamp = generate_dates_historic_vol(date_str)
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    hours = ["10","11","12","13","14","15"]
    for hour in hours:
        try:
            df = s3.get_object(Bucket="inv-alerts", Key=f"expanded_bf/vol/{key_str}/{hour}.csv")
            old_df = s3.get_object(Bucket="inv-alerts", Key=f"bf_mktHours/vol/{key_str}/{hour}.csv")
            df = pd.read_csv(df['Body'])
            old_df = pd.read_csv(old_df['Body'])
            new_df = pd.concat([old_df,df],ignore_index=True)
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf/vol/{key_str}/{hour}.csv", Body=new_df.to_csv())
        except Exception as e:
            print(e , f"for {key_str} {hour}")
    return put_response


if __name__ == "__main__":
    # build_historic_data(None, None)
    print(os.cpu_count())
    start_date = datetime(2024,4,8)
    end_date = datetime(2024,4,16)
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
    # run_process("2021-02-11")
        

    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
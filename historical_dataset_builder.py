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
from helpers.constants import *
import pandas_market_calendars as mcal
import numpy as np
import warnings

warnings.filterwarnings("ignore")

nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

alerts_bucket = os.getenv("ALERTS_BUCKET")

now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3')
logger = logging.getLogger()

def run_process(date_str):
    try:
        print(f"Starting {date_str}")
        build_historic_data(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        build_historic_data(date_str)
    print(f"Finished {date_str}")

def build_historic_data(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_np = np.datetime64(dt)
    if date_np in holidays_multiyear:
        return "holiday"
    for hour in hours:
        thirty_aggs, error_list = call_polygon_features(GE, from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
        df = feature_engineering(thirty_aggs,dt,hour)
        df.reset_index(drop=True, inplace=True)
        df = df.groupby("symbol").tail(1)
        result = df.apply(calc_price_action, axis=1)
        df = configure_price_features(df, result)
        df = configure_spy_features(df)
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"cdvol_GE/{key_str}/{hour}.csv", Body=df.to_csv())
    return put_response

def configure_price_features(df, result):
    result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
    result.reset_index()
    price = pd.DataFrame(result.to_list())
    df.reset_index(drop=True, inplace=True)
    df['one_max'] = price['one_max']
    df['one_min'] = price['one_min']
    df['one_pct'] = price['one_pct']
    df['three_max'] = price['three_max']
    df['three_min'] = price['three_min']
    df['three_pct'] = price['three_pct']
    return df
    
def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=8)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

def fix_data(date_str):
    s3 = get_s3_client()
    key_str = date_str.replace("-","/")
    for hour in ["10","11","12","13","14","15"]:
        try:
            df = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/new_features_expanded/{key_str}/{hour}.csv")
            data = pd.read_csv(df.get("Body"))
            data.rename(columns={'one_max':'three_max','one_min':'three_min','one_pct':'three_pct','three_max':'one_max','three_min':'one_min','three_pct':'one_pct'}, inplace=True)
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/new_features/{key_str}/{hour}.csv", Body=data.to_csv())
        except Exception as e:
            print(f"{date_str} {e}")
            continue
    print(f"Finished {date_str}")

if __name__ == "__main__":
    cpu = os.cpu_count()
    start_date = datetime(2016,9,16)
    end_date = datetime(2024,4,20)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    # fix_data("2015-01-08")

    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
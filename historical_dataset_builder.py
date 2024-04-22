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

high_vol = ['COIN','BILI','UPST','CVNA',"NIO","BABA","ROKU","RBLX","SE","SNAP","LCID","ZM","TDOC","UBER","RCL",
            'RIVN',"BIDU","FUTU","TSLA","JD","HOOD","CHWY","MARA","SNAP",'TWLO', 'DDOG', 'ZS', 'NET', 'OKTA',
            "DOCU",'SQ', 'SHOP',"PLTR","CRWD",'MRNA', 'SNOW', 'SOFI','LYFT','TSM','PINS','PANW','ORCL','SBUX','NKE',"UPS","FDX",
            'WDAY','SPOT']


def run_process(date_str):
    try:
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
        thirty_aggs, error_list = call_polygon_features(BF3, from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
        df = feature_engineering(thirty_aggs,dt,hour)
        df.reset_index(drop=True, inplace=True)
        df = df.groupby("symbol").tail(1)
        result = df.apply(calc_price_action, axis=1)
        df = configure_price_features(df, result)
        df = configure_spy_features(df)
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/new_features/{key_str}/{hour}.csv", Body=df.to_csv())
    return put_response
    
def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=8)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

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

def configure_spy_features(df):
    # spy,_ = call_polygon_features(["SPY"], from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
    # spy_features = feature_engineering(spy,datetime.strptime(to_stamp, "%Y-%m-%d"),hour)
    spy = df.loc[df['symbol'] == "SPY"]
    fived = spy["price_5Ddiff"].values[0]
    twentyd = spy["price_20Ddiff"].values[0]
    spy_range = spy["price_range"].values[0]
    df["SPY_5d"] = fived
    df["SPY_20d"] = twentyd 
    df["SPY_5d_diff"] = (df["price_5Ddiff"] - df["SPY_5d"])/df["price_5Ddiff"]
    df["SPY_20d_diff"] = (df["price_20Ddiff"] - df["SPY_20d"])/df["price_20Ddiff"]
    df["SPY_range_vol"] = spy_range
    return df


def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    hour_df = pd.concat(hour_aggregates)
    for index, value in enumerate(aggregates):
        try:
            hour_aggs = hour_df.loc[hour_df["symbol"] == value.iloc[0]['symbol']]
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
            full_aggs.append(value)
        except Exception as e:
            print(f"combine hour aggs {e}")
            print(value)
    return full_aggs




def pull_df(date_stamp, prefix, hour):
    date_stamp = date_stamp.replace("-","/")
    try: 
        dataset = s3.get_object(Bucket="inv-alerts", Key=f"{prefix}{date_stamp}/{hour}.csv")
        df = pd.read_csv(dataset.get("Body"))
    except Exception as e:
        print(f"{prefix}{date_stamp}/{hour}.csv")
        print(f"HERE {e}")
    return df


if __name__ == "__main__":
    cpu = os.cpu_count()
    start_date = datetime(2015,1,1)
    end_date = datetime(2024,4,14)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    # run_process("2024-04-15")

    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
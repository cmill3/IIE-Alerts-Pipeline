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
## add FB for historical

big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP","FB"
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","META",'QQQ','SPY','IWM'
            ]
indexes = ['QQQ','SPY','IWM']
memes = ['GME','AMC','MARA','TSLA','BBY','NIO','RIVN','XPEV','COIN','ROKU','LCID']
new_bf = ['C','CAT','KO','MS','GS','PANW','ORCL','IBM','CSCO','WMT','TGT','COST']
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3')
logger = logging.getLogger()


def run_process(date_str):
    try:
        build_vol_features(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        build_vol_features(date_str)
    print(f"Finished {date_str}")

def build_vol_features(date_str):
    print(date_str)
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    from_stamp, to_stamp, hour_stamp = generate_dates_historic_vol(date_str)

    for hour in hours:
        df = s3.get_object(Bucket="inv-alerts", Key=f"expanded_bf/{key_str}/{hour}.csv")
        df = pd.read_csv(df['Body'])
        min_aggs, error_list = call_polygon_vol(df['symbol'], from_stamp, to_stamp, timespan="minute", multiplier="1", hour=hour)
        hour_aggs, error_list = call_polygon_vol(df['symbol'], from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
        results_df = vol_feature_engineering(df, min_aggs, hour_aggs)
        csv = results_df.to_csv()
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf/vol/{key_str}/{hour}.csv", Body=csv)
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
    start_date = datetime(2018,1,1)
    end_date = datetime(2023,10,20)
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
    # build_vol_features("2022-01-27")
        

    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
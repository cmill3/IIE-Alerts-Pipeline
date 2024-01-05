import pandas as pd
import boto3
from helpers.aws import pull_files_s3, get_s3_client
import os
from datetime import timedelta, datetime
import concurrent.futures
import pandas_market_calendars as mcal

s3 = boto3.client('s3')

idx = ["QQQ","SPY","IWM"]

def run_process(date_str):
    try:
        print(f"Starting {date_str}")
        build_pcr_features(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        build_pcr_features(date_str)
    print(f"Finished {date_str}")


def generate_dates_historic_vol(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=4)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

def build_pcr_features(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    from_stamp, to_stamp, hour_stamp = generate_dates_historic_vol(date_str)

    for hour in hours:
        df = s3.get_object(Bucket="inv-alerts", Key=f"all_alerts/{key_str}/{hour}.csv")
        df = pd.read_csv(df['Body'])
        idx = df.loc[df['symbol'].isin(idx)]
        raw_pcr_data = pull_pcr_data(from_stamp,to_stamp,hour)
        pcr_df = pcr_feature_engineering(idx,raw_pcr_data)
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"idx_alerts/{key_str}/{hour}.csv", Body=pcr_df.to_csv())
    return "put_response"


def pull_pcr_data(from_stamp,to_stamp,hour):
    date_list = build_date_list(from_stamp,to_stamp)
    raw_pcr_data = {}
    for symbol in idx:
        dfs = []
        for date_str in date_list:
            key_str = date_str.replace("-","/")
            df = s3.get_object(Bucket="icarus-research-data", Key=f"options_snapshot/{key_str}/{hour}.csv")
            df = pd.read_csv(df['Body'])
            df['date'] = key_str
            df['date_hour'] = f"{key_str}-{hour}"
            dfs.append(df)
        full_sym = pd.concat(dfs)
        raw_pcr_data[symbol] = full_sym
    return raw_pcr_data

def build_date_list(from_stamp,to_stamp):
    start_date = datetime.strptime(from_stamp, "%Y-%m-%d")
    end_date = datetime.strptime(to_stamp, "%Y-%m-%d")
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    return date_list


def pcr_feature_engineering(raw_pcr_data):
    feature_data = {}
    for symbol in idx:
        sym_data = raw_pcr_data[symbol]
        hour = sym_data.iloc[-1]
        day = sym_data.iloc[-6:]
        ten_day = sym_data.iloc[-60:]



if __name__ == "__main__":
    # build_historic_data(None, None)
    print(os.cpu_count())
    date_list = build_date_list("2018-01-02","2023-12-23")
    # # for date_str in date_list:
    # run_process("2018-01-03")
        

    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
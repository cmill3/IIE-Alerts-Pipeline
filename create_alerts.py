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
from helpers.constants import WEEKLY_EXP, TRADING_SYMBOLS, TREND, PE2
warnings.filterwarnings("ignore")

alerts_bucket = os.getenv("ALERTS_BUCKET")

def run_process(date_str, dataset_title, data_filter):
    try:
        print(date_str)
        alerts_runner(date_str, dataset_title, data_filter)
    except Exception as e:
        print(f"{date_str} {e}")
        alerts_runner(date_str, dataset_title, data_filter)
    print(f"Finished {date_str}")

# def fix_alerts(date_str):
#     hours = ["10","11","12","13","14","15"]
#     key_str = date_str.replace("-","/")
#     s3 = boto3.client('s3')
#     from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
#     for hour in hours:
#         all_symbol = s3.get_object(Bucket="inv-alerts", Key=f"full_alerts/vol/{key_str}/{hour}.csv")
#         all_symbol_df = pd.read_csv(all_symbol['Body'])
#         all_symbol_df.drop(columns=['Unnamed: 0','Unnamed: 0.1'], inplace=True)
#         gm = all_symbol_df.loc[all_symbol_df['symbol'] == 'GM']
#         new_columns = [col.replace('_x', '') for col in all_symbol_df.columns]
#         all_symbol_df.columns = new_columns
#         y_columns = all_symbol_df.filter(regex='_y').columns
#         all_symbol_df.drop(columns=y_columns, inplace=True)
#         # new_gm = [col.replace('_y', '') for col in gm.columns]
#         # gm.columns = new_gm
#         # print(gm)
#         # print(all_symbol_df.head(3))
#         # df = pd.merge(all_symbol_df, gm, on=['symbol','hour','date'])
#         # df = df.drop_duplicates(subset=['symbol'])
#         put_response = s3.put_object(Bucket="inv-alerts", Key=f"all_alerts/vol_fix/{key_str}/{hour}.csv", Body=all_symbol_df.to_csv())

def alerts_runner(date_str, dataset_title, data_filter):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    for hour in hours:
        for minute in [0,30]:
            if minute == 30:
                all_symbol = s3.get_object(Bucket="inv-alerts", Key=f"trend_alerts/{key_str}/{hour}-{minute}.csv")
                all_symbol_df = pd.read_csv(all_symbol['Body'])
                all_symbol_df['minute'] = 30
            else:
                all_symbol = s3.get_object(Bucket="inv-alerts", Key=f"trend_alerts/{key_str}/{hour}.csv")
                all_symbol_df = pd.read_csv(all_symbol['Body'])
                all_symbol_df['minute'] = 0
            filtered = all_symbol_df.loc[all_symbol_df['symbol'].isin(data_filter)]
            trading_alerts = build_alerts(filtered)
            for alert in trading_alerts:
                df = trading_alerts[alert]
                if len(df) == 0:
                    continue
                csv = df.to_csv()
                if minute == 30:
                    put_response = s3.put_object(Bucket="inv-alerts", Key=f"trend_alerts/{dataset_title}/{key_str}/{alert}/{hour}-{minute}.csv", Body=csv)
                else:
                    put_response = s3.put_object(Bucket="inv-alerts", Key=f"trend_alerts/{dataset_title}/{key_str}/{alert}/{hour}.csv", Body=csv)


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
    df['cd_vol'] = (df['price_change_D']/df['return_vol_5D']).round(3)
    # df['cd_vol3'] = df['close_diff3']/df['return_vol_10D'].round(3)
    cdvol_gainers = df.loc[df['cd_vol'] > 0.36]
    cdvol_losers = df.loc[df['cd_vol'] < -0.21]
    volume_diff = df.loc[df['volume_14_56MA_diff'] > 0.078]
    # return {"cdvol_gainers": cdvol_gainers, "cdvol_losers": cdvol_losers, "cdvol3_gainers": cdvol3_gainers, "cdvol3_losers": cdvol3_losers}
    return {"cdvol_gainers": cdvol_gainers, "cdvol_losers": cdvol_losers, "volume_diff": volume_diff}

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
    data_filter = PE2
    dataset_title = "pe_sixty"
    start_date = datetime(2015,1,1)
    end_date = datetime(2024,8,1)
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
        

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str, dataset_title, data_filter) for date_str in date_list]
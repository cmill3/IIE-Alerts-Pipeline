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
from helpers.constants import WEEKLY_EXP, TRADING_SYMBOLS, TREND
warnings.filterwarnings("ignore")

alerts_bucket = os.getenv("ALERTS_BUCKET")

def run_process(date_str):
    try:
        print(date_str)
        alerts_runner(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        alerts_runner(date_str)
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

def alerts_runner(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        all_symbol = s3.get_object(Bucket="inv-alerts", Key=f"trend/all_symbols/{key_str}/{hour}.csv")
        all_symbol_df = pd.read_csv(all_symbol['Body'])
        all_symbol_df.drop(columns=['Unnamed: 0','Unnamed: 0.1'], inplace=True)
        # df = all_symbol_df.loc[all_symbol_df['symbol'].isin(["QQQ","SPY","IWM"])]
        # pcr = s3.get_object(Bucket="inv-alerts", Key=f"idx_alerts/{key_str}/{hour}/pcr_features.csv")
        # pcr_df = pd.read_csv(pcr['Body'])
        # pcr_df['symbol'] = pcr_df['Unnamed: 0']
        # df = df.merge(pcr_df, on=['symbol'])
        # new_columns = [col.replace('_x', '') for col in df.columns]
        # df.columns = new_columns
        # y_columns = df.filter(regex='_y').columns
        # df.drop(columns=y_columns, inplace=True)
        # put_response = s3.put_object(Bucket="inv-alerts", Key=f"idx_alerts/{key_str}/{hour}/full_data.csv", Body=df.to_csv())

        trading_df = all_symbol_df.loc[all_symbol_df['symbol'].isin(TREND)]
        # weekly_exp_df = all_symbol_df.loc[all_symbol_df['symbol'].isin(WEEKLY_EXP)]
        trading_alerts = build_alerts(trading_df)
        # weekly_exp_alerts = build_alerts(weekly_exp_df)
        for alert in trading_alerts:
            csv = trading_alerts[alert].to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"full_alerts/trend_alerts/{key_str}/{alert}/{hour}.csv", Body=csv)
        # for alert in weekly_exp_alerts:
        #     csv = weekly_exp_alerts[alert].to_csv()
        #     put_response = s3.put_object(Bucket="inv-alerts", Key=f"full_alerts/weekly_exp_alerts/{key_str}/{alert}/{hour}.csv", Body=csv)

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
    df['cd_vol'] = df['close_diff']/df['return_vol_10D'].round(3)
    df['cd_vol3'] = df['close_diff3']/df['return_vol_10D'].round(3)
    c_sorted = df.sort_values(by="close_diff", ascending=False)
    cvol_sorted = df.sort_values(by="cd_vol", ascending=False)
    cvol3_sorted = df.sort_values(by="cd_vol3", ascending=False)
    gainers = c_sorted.head(15)
    losers = c_sorted.tail(15)
    cdvol_gainers = cvol_sorted.head(15)
    cdvol_losers = cvol_sorted.tail(15)
    cdvol3_gainers = cvol3_sorted.head(15)
    cdvol3_losers = cvol3_sorted.tail(15)
    # volume = volume_sorted.head(15)
    # return {"cdvol_gainers": cdvol_gainers, "cdvol_losers": cdvol_losers, "cdvol3_gainers": cdvol3_gainers, "cdvol3_losers": cdvol3_losers}
    return {"gainers": gainers, "losers": losers,"cdvol_gainers": cdvol_gainers, "cdvol_losers": cdvol_losers, "cdvol3_gainers": cdvol3_gainers, "cdvol3_losers": cdvol3_losers}

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
    end_date = datetime(2024,1,27)
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
        

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
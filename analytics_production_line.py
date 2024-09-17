import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import *
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from botocore.exceptions import ClientError
from helpers.constants import TREND, BF3
import warnings 

warnings.filterwarnings("ignore")
alerts_bucket = os.getenv("ALERTS_BUCKET")
env = os.getenv("ENV")

s3 = get_s3_client()
est = pytz.timezone('US/Eastern')
date_est = datetime.now(est)
now_str = date_est.strftime("%Y/%m/%d/%H")
logger = logging.getLogger()

def cdvol_analytics_runner(event, context):
    year, month, day, hour = now_str.split("/")
    from_stamp, to_stamp = generate_dates_historic(date_est)
    dt = datetime.strptime(to_stamp, "%Y-%m-%d")
    thirty_aggs, error_list = call_polygon_features(BF3, from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour,month=month,day=day,year=year)
    df = feature_engineering(thirty_aggs,dt,hour)
    df.reset_index(drop=True, inplace=True)
    df = df.groupby("symbol").tail(1)
    df = configure_vti_features(df)
    df['hour'] = hour
    df = df.round(6)
    put_response = s3.put_object(Bucket=alerts_bucket, Key=f"production_alerts/{env}/{year}/{month}/{day}/{hour}.csv", Body=df.to_csv())
    return put_response

def trend_analytics_runner(event, context):
    minute = datetime.now(est).minute
    year, month, day, hour = now_str.split("/")
    from_stamp, to_stamp = generate_dates_historic(date_est)
    dt = datetime.strptime(to_stamp, "%Y-%m-%d")
    thirty_aggs, _ = call_polygon_features(TREND, from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour,month=month,day=day,year=year)
    df = feature_engineering(thirty_aggs,dt,hour)
    df.reset_index(drop=True, inplace=True)
    df = df.groupby("symbol").tail(1)
    df = configure_vti_features(df)
    df['hour'] = hour
    df['year'] = year
    df['day_of_week'] = dt.weekday()
    df['day_of_month'] = dt.day
    if minute >= 30:
        df['minute'] = 30
    else:
        df['minute'] = 0
    df = df.round(6)
    trading_alerts = build_alerts(df)
    for alert_type, alert_df in trading_alerts.items():
        csv = alert_df.to_csv()
        if minute >= 30:
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"live_alerts/{env}/trend_alerts/{alert_type}/{year}/{month}/{day}/{hour}-30.csv", Body=csv)
        else:
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"live_alerts/{env}/trend_alerts/{alert_type}/{year}/{month}/{day}/{hour}.csv", Body=csv)
    return put_response

def generate_dates_historic(date_est):
    start = date_est - timedelta(weeks=10)
    to_stamp = date_est.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp

def build_alerts(df):
    df['cd_vol'] = (df['price_change_D']/df['return_vol_5D']).round(3)
    cdvol_gainers = df.loc[df['cd_vol'] > 0.36]
    cdvol_losers = df.loc[df['cd_vol'] < -0.21]
    volume_diff = df.loc[df['volume_14_56MA_diff'] > 0.078]
    return {"cdvol_gainers": cdvol_gainers, "cdvol_losers": cdvol_losers, "volume_diff": volume_diff}

if __name__ == "__main__":
    cdvol_analytics_runner(None,None)
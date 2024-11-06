import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import *
from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
import logging
from helpers.constants import *
from helpers.historical_data_helpers import call_polygon_features_historical
import warnings

warnings.filterwarnings("ignore")
alerts_bucket = os.getenv("ALERTS_BUCKET")
env = os.getenv("ENV")


s3 = get_s3_client()
est = pytz.timezone('US/Eastern')
date_est = datetime.now(est)
now_str = date_est.strftime("%Y/%m/%d/%H")
logger = logging.getLogger()

def run_validation(event, context):
    prices = []
    year, month, day, hour = now_str.split("/")
    stamp  = f"{year}-{month}-{day}"
    logger.info(f"Running validation for {stamp} {hour}")
    alert_df = s3.get_object(Bucket=alerts_bucket, Key=f"production_alerts/{env}/{year}/{month}/{day}/{hour}.csv")
    for stock in BF3:
        try:
            df = polygon_call_stocks(stock, stamp, stamp, "5","minute")
            df = df.loc[df['hour'] == int(hour)]
            df = df.loc[df['minute'] == 0]
            open = df['o'].values[0]
            prices.append({"symbol":stock,"validation_price":open})
        except Exception as e:
            raise ValueError(f"Error in {stock} {e}")
    
    alert_data = pd.read_csv(alert_df.get("Body"))
    alert_data['validation_price'] = alert_data['symbol'].apply(lambda x: [i['validation_price'] for i in prices if i['symbol'] == x][0])
    alert_data['validation_pct'] = abs((alert_data['validation_price'] - alert_data['alert_price'])/alert_data['alert_price'])
    alert_data['validation'] = alert_data['validation_pct'].apply(lambda x: "PASS" if x < 0.01 else "FAIL")
    logger.info(f"Validation: {alert_data['validation'].value_counts()}")
    logger.info(f"Validation PCT: {alert_data[['validation_pct','symbol']]}")
    s3.put_object(Bucket=alerts_bucket, Key=f"data_validations/{env}/{year}/{month}/{day}/{hour}_validation.csv", Body=alert_data.to_csv())
    if alert_data['validation'].str.contains("FAIL").any():
        raise ValueError("Validation failed")
    

def convert_timestamp_est(timestamp):
    # Create a UTC datetime object from the timestamp
    utc_datetime = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)
    # Define the EST timezone
    est_timezone = pytz.timezone('America/New_York')
    # Convert the UTC datetime to EST
    est_datetime = utc_datetime.astimezone(est_timezone)
    return est_datetime


def polygon_call_stocks(contract, from_stamp, to_stamp, multiplier, timespan):
    try:
        payload={}
        headers = {}
        url = f"https://api.polygon.io/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"
        print(url)
        response = requests.request("GET", url, headers=headers, data=payload)
        res_df = pd.DataFrame(json.loads(response.text)['results'])
        res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
        res_df['date'] = res_df['t'].apply(lambda x: convert_timestamp_est(x))
        res_df['time'] = res_df['date'].apply(lambda x: x.time())
        res_df['hour'] = res_df['date'].apply(lambda x: x.hour)
        res_df['minute'] = res_df['date'].apply(lambda x: x.minute)
        res_df['symbol'] = contract
        print(res_df)
        return res_df
    except Exception as e:  
        print(e)
        return pd.DataFrame()

# def fix_data(date_str):
#     s3 = get_s3_client()
#     key_str = date_str.replace("-","/")
#     for hour in ["10","11","12","13","14","15"]:
#         try:
#             df = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/new_features_expanded/{key_str}/{hour}.csv")
#             data = pd.read_csv(df.get("Body"))
#             data.rename(columns={'one_max':'three_max','one_min':'three_min','one_pct':'three_pct','three_max':'one_max','three_min':'one_min','three_pct':'one_pct'}, inplace=True)
#             put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/new_features/{key_str}/{hour}.csv", Body=data.to_csv())
#         except Exception as e:
#             print(f"{date_str} {e}")
#             continue
#     print(f"Finished {date_str}")

if __name__ == "__main__":
    run_validation(None,None)
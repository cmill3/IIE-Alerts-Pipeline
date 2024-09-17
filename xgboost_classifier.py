from fileinput import filename
import numpy as np
import boto3
import json
import pandas as pd
import datetime 
import os
import ast
from datetime import datetime
from helpers.constants import FEATURES, ENDPOINT_NAMES, PE2, TOP
from helpers.helper import pull_model_config
import pytz 


s3 = boto3.client('s3')
runtime = boto3.client("sagemaker-runtime")


predictions_bucket = os.getenv("PREDICTIONS_BUCKET")
alerts_bucket = os.getenv("ALERTS_BUCKET")
strategies = os.getenv("STRATEGIES")
env = os.getenv("ENV")
portfolio_strategy = os.getenv("PORTFOLIO_STRATEGY")

est = pytz.timezone('US/Eastern')
date = datetime.now(est)
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
minute = date.minute

def invoke_model(event, context):   
    strategies_list = strategies.split(",")
    year, month, day, hour = format_dates(date)
    key = format_key(year, month, day, hour)
    dataset = s3.get_object(
        Bucket=alerts_bucket,
        Key=key)
    data = pd.read_csv(dataset.get("Body"))

    data['dt'] = pd.to_datetime(data['date'])
    recent_date = data['dt'].iloc[-1]
    data['day_of_week'] = data['dt'].apply(lambda x: x.dayofweek).astype(int)
    data['day_of_month'] = data['dt'].apply(lambda x: x.day).astype(int)
    data['month'] = data['dt'].apply(lambda x: x.month).astype(int)
    data['year'] = data['dt'].apply(lambda x: x.year).astype(int)
    data['cd_vol'] = (data['price_change_D']/data['return_vol_5D']).round(3)
    data['cd_vol3'] = (data['price_3Ddiff']/data['return_vol_5D']).round(3)
    data['DMplus'] = data.apply(lambda x: 1 if x['DMplus'] == 'TRUE' else 0, axis=1)
    data['DMminus'] = data.apply(lambda x: 1 if x['DMminus'] == 'TRUE' else 0, axis=1)
    
    for strategy in strategies_list:
        endpoint_name = ENDPOINT_NAMES[strategy]
        prediciton_data = data[FEATURES]
        prediction_csv = prediciton_data.to_csv(header=False,index=False).encode()
        response = runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType="text/csv",
            Body=prediction_csv
        )
        t = response['Body']
        results = t.read()

        results_df = format_result(results, data['symbol'].to_list(), recent_date, data, strategy)
        results_csv = results_df.to_csv().encode()
        
        try:
            if minute >= 30:
                s3.put_object(Bucket=predictions_bucket, Key=f'classifier_predictions/{env}/{strategy}/{year}/{month}/{day}/{hour}-30.csv', Body=results_csv)
            else:
                s3.put_object(Bucket=predictions_bucket, Key=f'classifier_predictions/{env}/{strategy}/{year}/{month}/{day}/{hour}.csv', Body=results_csv)
        except:
            print("error")
            print(strategy)
            continue
    return "put_response"

def format_key(year, month, day, hour):
    if portfolio_strategy == "CDVOL_GAIN":
        if minute >= 30:
            key = f"live_alerts/{env}/trend_alerts/cdvol_gainers/{year}/{month}/{day}/{hour}-30.csv"
        else:
            key = f"live_alerts/{env}/trend_alerts/cdvol_gainers/{year}/{month}/{day}/{hour}.csv"
    elif portfolio_strategy == "CDVOL_LOSE":
        if minute >= 30:
            key = f"live_alerts/{env}/trend_alerts/cdvol_losers/{year}/{month}/{day}/{hour}-30.csv"
        else:
            key = f"live_alerts/{env}/trend_alerts/cdvol_losers/{year}/{month}/{day}/{hour}.csv"

    return key
    
def format_result(result_string, symbol_list, recent_date, data, strategy) -> pd.DataFrame:
    model_config = pull_model_config(strategy)
    try:
        result_string = result_string.decode("utf-8") 
        array = result_string.split(",")
        results_df = pd.DataFrame({'classifier_prediction': array})
        results_df['symbol'] = symbol_list
        results_df['recent_date'] = recent_date
        results_df['return_vol_5D'] = data['return_vol_5D']
        results_df['return_vol_10D'] = data['return_vol_10D']
        results_df['target_pct'] = model_config['target_value']
        return results_df
    except Exception as e:
        print(e)

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour)
    return year, month, day, hour

if __name__ == "__main__":
    invoke_model(None,None)
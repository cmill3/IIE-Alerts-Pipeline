from fileinput import filename
import numpy as np
import boto3
import json
import pandas as pd
import datetime 
import os
import ast
from datetime import datetime
from helpers.constants import MODEL_FEATURES, ENDPOINT_NAMES
from helpers.helper import pull_model_config
import pytz 


s3 = boto3.client('s3')
runtime = boto3.client("sagemaker-runtime")


predictions_bucket = os.getenv("PREDICTIONS_BUCKET")
alerts_bucket = os.getenv("ALERTS_BUCKET")
strategies = os.getenv("STRATEGIES")
alert_type = os.getenv("ALERT_TYPE")

est = pytz.timezone('US/Eastern')
date = datetime.now(est)
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")

def invoke_model(event, context):   
    strategies_list = strategies.split(",")
    year, month, day, hour = format_dates(date)
    dataset = s3.get_object(Bucket=alerts_bucket, Key=f"inv_alerts/production_alerts/{year}/{month}/{day}/{alert_type}/{hour}.csv")
    data = pd.read_csv(dataset.get("Body"))

    data['dt'] = pd.to_datetime(data['date'])
    recent_date = data['dt'].iloc[-1]
    data['roc_diff'] = data['roc'] - data['roc5']
    data['cd_vol'] = (data['close_diff'] / data['return_vol_10D']).round(3)
    data['cd_vol3'] = (data['close_diff3'] / data['return_vol_10D']).round(3)
    data['range_vol_diff5'] = (data['range_vol'] - data['range_vol5MA'])
    data['close_diff_deviation3'] = abs(data['close_diff3'])/(data['threeD_stddev50']*100)
    data['close_diff_deviation'] = abs(data['close_diff'])/(data['oneD_stddev50']*100)
    data['day_of_week'] = data['dt'].apply(lambda x: x.dayofweek).astype(int)
    data['day_of_month'] = data['dt'].apply(lambda x: x.day).astype(int)
    data['month'] = data['dt'].apply(lambda x: x.month).astype(int)
    data['year'] = data['dt'].apply(lambda x: x.year).astype(int)
    if alert_type == "losers":
        alerts = data.tail(6)
    else:
        alerts = data.head(6)

    
    for strategy in strategies_list:
        endpoint_name = ENDPOINT_NAMES[strategy]
        prediciton_data = data[MODEL_FEATURES[strategy]]
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
            put_response = s3.put_object(Bucket=predictions_bucket, Key=f'classifier_predictions/{strategy}/{year}/{month}/{day}/{hour}.csv', Body=results_csv)
        except:
            print("error")
            print(strategy)
            continue
    return put_response
    
def format_result(result_string, symbol_list, recent_date, data, strategy) -> pd.DataFrame:
    model_config = pull_model_config(strategy)
    try:
        result_string = result_string.decode("utf-8") 
        array = result_string.split(",")
        results_df = pd.DataFrame({'classifier_prediction': array})
        results_df['symbol'] = symbol_list
        results_df['recent_date'] = recent_date
        results_df['return_vol_10D'] = data['return_vol_10D']
        results_df['return_vol_30D'] = data['return_vol_30D']
        results_df['target_pct'] = model_config['target_value']
        return results_df
    except Exception as e:
        print(e)

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour)
    return year, month, day, hour
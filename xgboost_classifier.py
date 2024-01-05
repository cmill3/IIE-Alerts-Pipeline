from fileinput import filename
import numpy as np
import boto3
import json
import pandas as pd
import datetime 
import os
import ast
from datetime import datetime
from helpers.constants import MODEL_FEATURES


s3 = boto3.client('s3')
runtime = boto3.client("sagemaker-runtime")


predictions_bucket = os.getenv("PREDICTIONS_BUCKET")
alerts_bucket = os.getenv("ALERTS_BUCKET")
title = os.getenv("TITLE")


big_fish =  ["AMD","NVDA","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP","CRM",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI",'META',
            'C','TGT','MMM','SQ','PANW','DAL','CSCO','UBER']
indexes = ['QQQ','SPY','IWM']
index_strategies = ["indexP_1d","indexC_1d","indexP","indexC"]
bf_strategies = ["bfP_1d","bfC_1d","bfP","bfC"]

def invoke_model(event, context):   
    endpoint_names = os.getenv("ENDPOINT_NAMES")
    endpoint_names = endpoint_names.split(",")
    year, month, day, hour = format_dates(datetime.now())
    keys = s3.list_objects(Bucket=alerts_bucket,Prefix=f"bf_alerts/{year}/{month}/{day}")["Contents"]
    query_key = keys[-1]['Key']
    dataset = s3.get_object(Bucket=alerts_bucket, Key=query_key)
    data = pd.read_csv(dataset.get("Body"))

    data['dt'] = pd.to_datetime(data['date'])
    recent_date = data['dt'].iloc[-1]
    data['roc_diff'] = data['roc'] - data['roc5']
    data['range_vol_diff5'] = (data['range_vol'] - data['range_vol5MA'])
    data['close_diff_deviation3'] = abs(data['close_diff3'])/(data['threeD_stddev50']*100)
    data['close_diff_deviation'] = abs(data['close_diff'])/(data['oneD_stddev50']*100)
    data['day_of_week'] = data['dt'].apply(lambda x: x.dayofweek).astype(int)
    data['day_of_month'] = data['dt'].apply(lambda x: x.day).astype(int)
    data['month'] = data['dt'].apply(lambda x: x.month).astype(int)
    data['year'] = data['dt'].apply(lambda x: x.year).astype(int)

    if title in bf_strategies:
        data = data[data['symbol'].isin(big_fish)]
        symbol_list = data['symbol']
    elif title in index_strategies:
        data = data[data['symbol'].isin(indexes)]
        symbol_list = data['symbol']

    
    for name in endpoint_names:
        strat_name = name.strip('"')
        prediciton_data = data[MODEL_FEATURES[strat_name]]
        prediction_csv = prediciton_data.to_csv(header=False,index=False).encode()
        response = runtime.invoke_endpoint(
            EndpointName=strat_name,
            ContentType="text/csv",
            Body=prediction_csv
        )
        t = response['Body']
        results = t.read()

        results_df = format_result(results, symbol_list, recent_date)
        results_csv = results_df.to_csv().encode()
        
        try:
            put_response = s3.put_object(Bucket=predictions_bucket, Key=f'classifier_predictions/{strat_name}/{query_key}', Body=results_csv)
        except:
            print("error")
            print(name)
            continue
    return put_response
    
def format_result(result_string, symbol_list, recent_date) -> pd.DataFrame:
    try:
        result_string = result_string.decode("utf-8") 
        array = result_string.split(",")
        results_df = pd.DataFrame({'classifier_prediction': array})
        results_df['symbol'] = symbol_list.values
        results_df['recent_date'] = recent_date

        return results_df
    except Exception as e:
        print(e)

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour) - 4
    return year, month, day, hour

if __name__ == "__main__":
    year, month, day, hour = format_dates(datetime.now())
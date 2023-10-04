from fileinput import filename
import numpy as np
import boto3
import json
import pandas as pd
import datetime 
import os
import ast
from datetime import datetime


s3 = boto3.client('s3')
runtime = boto3.client("sagemaker-runtime")


predictions_bucket = os.getenv("PREDICTIONS_BUCKET")
alerts_bucket = os.getenv("ALERTS_BUCKET")
title = os.getenv("TITLE")

# def run(event, context):
#     endpoint_names = ast.literal_eval(endpoint_names)
#     for name in endpoint_names:
#         invoke_model(name)

features = {
    "invalerts-xgb-bfc-classifier": ['price_25MA', 'close_diff', 'SPY_5D', 'range_vol25MA', 'SPY_diff3', 'close_diff_deviation', 'v_diff_pct', 'month', 'range_vol10MA', 
                                     'threeD_stddev50', 'SPY_1D', 'range_vol', 'volume_25MA', 'adjusted_volume', 'roc3', 'cmf', 'range_vol5MA', 'day_of_month', 'roc5', 'SPY_diff'],
    "invalerts-xgb-bfp-classifier": ['price7', 'month', 'adx', 'price_10MA', 'adjusted_volume', 'hour', 'threeD_stddev50', 'price_10DDiff', 'volume_10DDiff', 'close_diff', 
                                     'range_vol', 'range_vol5MA', 'volume_10MA', 'close_diff5', 'volume_25DDiff', 'SPY_diff'],
    "invalerts-xgb-indexc-classifier": ['range_vol10MA', 'volume_10MA', 'price_10MA', 'volume_25DDiff', 'roc5', 'rsi', 'close_diff_deviation3', 'roc_diff', 'volume_10DDiff', 
                                        'vol14', 'close_diff_deviation', 'price14', 'cmf', 'price7', 'range_vol', 'month'],
    "invalerts-xgb-indexp-classifier": ['price7', 'price_25MA', 'range_vol5MA', 'close_diff_deviation3', 'range_vol25MA', 'adx', 'close_diff5', 'threeD_stddev50', 
                                        'oneD_returns_close', 'oneD_stddev50', 'range_vol_diff5'],
    "invalerts-xgb-bfc-1d-classifier": ['range_vol_diff5', 'close_diff_deviation', 'range_vol5MA', 'SPY_5D', 'volume_25DDiff', 'range_vol10MA', 'volume_25MA', 'range_vol25MA', 
                                        'roc', 'SPY_diff', 'volume_10MA', 'roc5', 'roc_diff', 'price_10DDiff', 'rsi3', 'roc3', 'adjusted_volume', 'month', 'cmf', 'day_of_week', 'close_diff_deviation3', 'v_diff_pct', 'close_diff', 'rsi'],
    "invalerts-xgb-bfp-1d-classifier": ['roc3', 'oneD_returns_close', 'v_diff_pct', 'threeD_stddev50', 'range_vol_diff5', 'rsi5', 'close_diff3', 'close_diff_deviation3', 
                                        'vol14', 'hour', 'roc_diff', 'threeD_returns_close', 'SPY_diff5', 'range_vol', 'price14', 'rsi', 'roc5', 'SPY_diff', 'rsi3'],
    "invalerts-xgb-indexc-1d-classifier": ['roc_diff', 'close_diff5', 'range_vol10MA', 'adx', 'close_diff_deviation3', 'close_diff3', 'rsi5', 'adjusted_volume', 'volume_25DDiff', 
                                           'day_of_month', 'cmf', 'volume_10DDiff', 'range_vol', 'range_vol25MA', 'roc', 'threeD_returns_close', 'oneD_stddev50', 'oneD_returns_close'],
    "invalerts-xgb-indexp-1d-classifier": ['price7', 'price_25MA', 'range_vol5MA', 'close_diff_deviation3', 'range_vol25MA', 'adx', 'close_diff5', 'threeD_stddev50', 
                                           'oneD_returns_close', 'oneD_stddev50', 'range_vol_diff5'],
            
}

big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","FB","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","FB","SNOW","SOFI",
            "UAL","DAL","AAL"
            ]
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

    dates = data['date'].unique()
    recent_date = dates[-1]
    data['dt'] = pd.to_datetime(data['date'])
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
        prediciton_data = data[features[strat_name]]
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
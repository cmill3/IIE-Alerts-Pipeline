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
    "invalerts-xgb-bfc-classifier": ['volume_10MA', 'threeD_returns_close', 'SPY_diff', 'range_vol_diff5', 'price_25MA', 'month', 'hour', 'oneD_stddev50',
                                      'close_diff_deviation3', 'roc5', 'SPY_diff5', 'roc3', 'adx', 'vol14', 'close_diff', 'day_of_month', 'roc_diff', 'range_vol', 'adjusted_volume'],
    "invalerts-xgb-bfp-classifier": ['close_diff_deviation', 'volume_10DDiff', 'vol14', 'volume_25MA', 'v_diff_pct', 'threeD_returns_close', 'SPY_1D', 'volume_10MA', 'range_vol25MA', 'price14'],
    "invalerts-xgb-indexc-classifier": ['vol14', 'hour', 'roc', 'volume_25DDiff', 'range_vol_diff5', 'threeD_stddev50', 'close_diff5', 'range_vol10MA', 'price14', 'day_of_week', 'price_10MA'],
    "invalerts-xgb-indexp-classifier": ['price_25MA', 'year', 'rsi5', 'hour', 'oneD_stddev50', 'roc5', 'adjusted_volume', 'roc3', 'volume_25DDiff', 'close_diff3'],
    "invalerts-xgb-bfc-1d-classifier": ['price_25MA', 'adjusted_volume', 'price_25DDiff', 'volume_10DDiff', 'range_vol5MA', 'vol14', 'price_10MA', 'close_diff', 'volume_25DDiff', 'month', 'hour', 'rsi'],
    "invalerts-xgb-bfp-1d-classifier": ['roc', 'close_diff5', 'SPY_3D', 'SPY_diff', 'volume_25DDiff', 'range_vol_diff5', 'volume_10DDiff', 'range_vol25MA', 'adjusted_volume', 'range_vol'],
    "invalerts-xgb-indexc-1d-classifier": ['vol14', 'hour', 'roc', 'volume_25DDiff', 'range_vol_diff5', 'threeD_stddev50', 'close_diff5', 'range_vol10MA', 'price14', 'day_of_week', 'price_10MA'],
    "invalerts-xgb-indexp-1d-classifier": ['price_25MA', 'year', 'rsi5', 'hour', 'oneD_stddev50', 'roc5', 'adjusted_volume', 'roc3', 'volume_25DDiff', 'close_diff3'],
            
}

big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","FB","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","FB","SNOW","T","VZ","SOFI",
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
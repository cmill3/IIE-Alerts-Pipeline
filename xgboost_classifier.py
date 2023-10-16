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
    "invalerts-xgb-bfc-classifier": ['return_vol_240M', 'return_vol_450M', 'price_10DDiff', 'threeD_returns_close', 'cmf', 'volume_vol_240M', 'close_diff', 'volume_vol_16H', 'return_vol_5D', 
                                    'oneD_stddev50', 'rsi3', 'return_vol_16H', 'hour', 'daily_volume_vol_diff_pct', 'volume_vol_450M', 'return_vol_30D', 'month', 'roc5', 'volume_vol_10D', 'day_of_week', 
                                    'close_diff5', 'range_vol_diff5', 'min_vol_diff_pct', 'hour_volume_vol_diff_pct', 'roc', 'rsi5', 'price7', 'volume_vol_30D', 'volume_vol_8H', 'daily_vol_diff_pct', 'v_diff_pct', 
                                    'roc3', 'range_vol', 'volume_vol_5D', 'daily_volume_vol_diff_pct30'],
    "invalerts-xgb-bfp-classifier": ['volume_vol_16H', 'volume_vol_30D', 'oneD_returns_close', 'roc_diff', 'day_of_week', 'daily_volume_vol_diff_pct30', 'threeD_stddev50', 'range_vol', 
                                    'hour_vol_diff_pct', 'oneD_stddev50', 'min_vol_diff_pct', 'month', 'daily_vol_diff_pct', 'close_diff', 'volume_vol_240M', 'return_vol_10D', 'return_vol_30D', 
                                    'volume_vol_450M', 'adx', 'daily_vol_diff_pct30', 'return_vol_450M', 'rsi5', 'return_vol_240M', 'threeD_returns_close', 'return_vol_5D', 'rsi', 'roc5', 'close_diff3',
                                    'v_diff_pct', 'roc3', 'day_of_month', 'cmf', 'hour_volume_vol_diff_pct'],
    "invalerts-xgb-indexc-classifier": ['daily_vol_diff_pct30', 'close_diff', 'return_vol_240M', 'oneD_returns_close', 'day_of_month', 'return_vol_16H', 'return_vol_450M', 'volume_vol_5D',
                                        'threeD_returns_close', 'hour_volume_vol_diff_pct', 'hour_vol_diff_pct', 'range_vol', 'return_vol_8H', 'rsi3', 'hour', 'daily_volume_vol_diff_pct30', 
                                        'volume_vol_10D', 'roc_diff', 'return_vol_5D', 'volume_vol_16H', 'return_vol_30D', 'close_diff5', 'price_10DDiff', 'volume_vol_240M', 'roc', 'rsi'],
    "invalerts-xgb-indexp-classifier": ['volume_vol_240M', 'roc3', 'close_diff', 'volume_vol_10D', 'daily_volume_vol_diff_pct', 'daily_volume_vol_diff_pct30', 'return_vol_450M', 
                                        'range_vol', 'return_vol_240M', 'volume_vol_5D', 'day_of_month', 'day_of_week', 'cmf', 'adx', 'oneD_returns_close', 'month', 'daily_vol_diff_pct', 'hour_vol_diff_pct', 
                                        'roc_diff', 'close_diff5', 'min_volume_vol_diff_pct', 'range_vol_diff5', 'volume_vol_30D', 'rsi', 'close_diff_deviation', 'v_diff_pct', 'rsi5', 'roc', 'oneD_stddev50', 
                                        'return_vol_10D', 'return_vol_16H', 'min_vol_diff_pct', 'return_vol_5D', 'return_vol_30D'],
    "invalerts-xgb-bfc-1d-classifier": ['close_diff_deviation', 'vol7', 'daily_volume_vol_diff30', 'price_10DDiff', 'close_diff3', 'return_vol_30D', 
                                        'hour_vol_diff_pct', 'v_diff_pct', 'month', 'hour_volume_vol_diff', 'rsi5', 'range_vol_diff5', 'daily_vol_diff30',
                                         'rsi', 'roc_diff', 'range_vol5MA', 'price_25MA', 'year', 'min_vol_diff', 'return_vol_5D', 'daily_vol_diff', 'price14', 
                                         'adx', 'volume_10MA'],
    "invalerts-xgb-bfp-1d-classifier": ['cmf', 'range_vol10MA', 'daily_vol_diff30', 'adx', 'daily_volume_vol_diff', 'min_volume_vol_diff', 'threeD_stddev50', 'range_vol5MA', 'oneD_stddev50', 
                                        'price_10MA', 'close_diff5', 'daily_vol_diff_pct30', 'hour_vol_diff', 'hour_vol_diff_pct'],
    "invalerts-xgb-indexc-1d-classifier": ['return_vol_450M', 'volume_vol_10D', 'rsi3', 'volume_vol_30D', 'day_of_month', 'min_vol_diff_pct', 'price7', 'adx', 'return_vol_5D', 'return_vol_16H', 
                                            'close_diff', 'min_volume_vol_diff_pct', 'daily_volume_vol_diff_pct30', 'volume_vol_8H', 'roc5', 'threeD_returns_close', 'volume_vol_16H', 'volume_vol_240M', 
                                            'v_diff_pct', 'hour', 'volume_vol_5D', 'hour_volume_vol_diff_pct', 'hour_vol_diff_pct', 'daily_vol_diff_pct30', 'roc_diff', 'rsi5', 'price_10DDiff', 'range_vol', 
                                            'oneD_stddev50', 'roc3', 'price_25DDiff', 'cmf', 'close_diff_deviation', 'close_diff3', 'month', 'range_vol_diff5'],
    "invalerts-xgb-indexp-1d-classifier": ['v_diff_pct', 'close_diff', 'hour_vol_diff_pct', 'oneD_stddev50', 'return_vol_5D', 'close_diff3', 'return_vol_450M', 'cmf', 'close_diff5', 
                                            'return_vol_30D', 'return_vol_8H', 'range_vol', 'threeD_returns_close', 'rsi5', 'roc3', 'volume_vol_16H', 'volume_vol_30D', 'hour', 'daily_vol_diff_pct', 
                                            'return_vol_240M', 'price_25DDiff', 'day_of_week', 'volume_vol_5D', 'min_vol_diff_pct']
            
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

    data['dt'] = pd.to_datetime(data['date_x'])
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
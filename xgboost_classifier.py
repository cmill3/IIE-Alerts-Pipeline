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
    "invalerts-xgb-losers-classifier": ['rsi','roc','roc3','close_diff','price_10DDiff','close_diff3',
                                        'SPY_diff','SPY_diff3','SPY_1D','SPY_3D'],
    "invalerts-xgb-gainers-classifier": ['rsi','roc','roc3','close_diff','price_10DDiff','close_diff3',
                                        'SPY_diff','SPY_diff3','SPY_1D','SPY_3D'],
    "invalerts-xgb-MA-classifier": ['rsi', 'roc', 'roc5', 'v', 'price_10DDiff', 'price_25DDiff', 'close_diff', 'v_diff_pct', 
                                        'rsi5', 'close_diff5', 'SPY_diff', 'SPY_diff5', 'SPY_1D', 'SPY_5D'],
    "invalerts-xgb-MAP-classifier": ['rsi','roc','roc3','close_diff','price_10DDiff','close_diff3',
                                    'SPY_diff','SPY_diff3','SPY_1D','SPY_3D'],
    "invalerts-xgb-vdiff-gainC-classifier": ['rsi', 'roc', 'roc5', 'v', 'price_10DDiff', 'price_25DDiff', 'close_diff', 'v_diff_pct', 
                                        'rsi5', 'close_diff5', 'SPY_diff', 'SPY_diff5', 'SPY_1D', 'SPY_5D'],
    "invalerts-xgb-vdiff-gainP-classifier": ['rsi', 'roc', 'roc5', 'v', 'price_10DDiff', 'price_25DDiff', 'close_diff', 'v_diff_pct', 
                                        'rsi5', 'close_diff5', 'SPY_diff', 'SPY_diff5', 'SPY_1D', 'SPY_5D'],
    "invalerts-xgb-bfc-classifier": ['rsi', 'cmf', 'SPY_diff', 'roc', 'roc3', 'roc5', 'price_10DDiff', 'price_25DDiff', 'v_diff_pct', 'v', 
                                     'close_diff', 'roc_diff', 'day_of_week', 'hour', 'volume_10DDiff', 'month', 'threeD_stddev50', 'range_vol'],
    "invalerts-xgb-bfp-classifier": ['rsi', 'cmf', 'SPY_diff', 'roc', 'roc3', 'roc5', 'price_10DDiff', 'price_25DDiff', 'v_diff_pct', 'v', 'close_diff', 
                                     'roc_diff', 'day_of_week', 'hour', 'volume_10DDiff', 'month', 'threeD_stddev50', 'range_vol', 'close_diff_deviation3']
            
}


def invoke_model(event, context):   
    endpoint_names = os.getenv("ENDPOINT_NAMES")
    endpoint_names = endpoint_names.split(",")
    year, month, day, hour = format_dates(datetime.now())
    keys = s3.list_objects(Bucket=alerts_bucket,Prefix=f"bf_alerts/{year}/{month}/{day}")["Contents"]
    query_key = keys[-1]['Key']
    print(query_key)
    dataset = s3.get_object(Bucket=alerts_bucket, Key=query_key)
    df = pd.read_csv(dataset.get("Body"))

    dates = df['date'].unique()
    recent_date = dates[-1]
    symbol_list = df['symbol']
    df['dt'] = pd.to_datetime(df['date'])
    df['day_of_week'] = df['dt'].apply(lambda x: x.dayofweek)
    df['hour'] = df['dt'].apply(lambda x: x.hour)
    df['roc_diff'] = df['roc'] - df['roc5']
    df['month'] = df['dt'].apply(lambda x: x.month)
    df['close_diff_deviation3'] = abs(df['close_diff3'])/(df['threeD_stddev50']*100)

    
    for name in endpoint_names:
        print(name)
        # strategy_name = name.split("-")[2]
        strat_name = name.strip('"')
        prediciton_data = df[features[strat_name]]
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
        results_df['symbol'] = symbol_list
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
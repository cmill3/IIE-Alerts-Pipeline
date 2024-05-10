import boto3
import pandas as pd
from datetime import datetime
import os

s3 = boto3.client('s3')


trading_bucket = os.getenv("TRADING_BUCKET")

def aggregate(event, context):
    gainers_keys = s3.list_objects(Bucket=trading_bucket,Prefix="classifier_predictions/invalerts-xgb-gainers-classifier/")["Contents"]
    gainers_key = gainers_keys[-1]['Key']
    gainers_dataset = s3.get_object(Bucket=trading_bucket, Key=gainers_key)
    gainers_df = pd.read_csv(gainers_dataset.get("Body"))
    gainers_df['strategy'] = 'day_gainers'
    gainers_df.drop(['Unnamed: 0'],axis=1,inplace=True)

    losers_keys = s3.list_objects(Bucket=trading_bucket,Prefix="classifier_predictions/invalerts-xgb-losers-classifier/")["Contents"]
    losers_key = losers_keys[-1]['Key']
    losers_dataset = s3.get_object(Bucket=trading_bucket, Key=losers_key)
    losers_df = pd.read_csv(losers_dataset.get("Body"))
    losers_df['strategy'] = 'day_losers'
    losers_df.drop(['Unnamed: 0'],axis=1,inplace=True)

    ma_keys = s3.list_objects(Bucket=trading_bucket,Prefix="classifier_predictions/invalerts-xgb-MA-classifier/")["Contents"]
    ma_key = ma_keys[-1]['Key']
    ma_dataset = s3.get_object(Bucket=trading_bucket, Key=ma_key)
    ma_df = pd.read_csv(ma_dataset.get("Body"))
    ma_df['strategy'] = 'most_actives'
    ma_df.drop(['Unnamed: 0'],axis=1,inplace=True)

    maP_keys = s3.list_objects(Bucket=trading_bucket,Prefix="classifier_predictions/invalerts-xgb-MAP-classifier/")["Contents"]
    maP_key = maP_keys[-1]['Key']
    maP_dataset = s3.get_object(Bucket=trading_bucket, Key=maP_key)
    maP_df = pd.read_csv(maP_dataset.get("Body"))
    maP_df['strategy'] = 'maP'
    maP_df.drop(['Unnamed: 0'],axis=1,inplace=True)

    vdC_keys = s3.list_objects(Bucket=trading_bucket,Prefix="classifier_predictions/invalerts-xgb-vdiff-gainC-classifier/")["Contents"]
    vdC_key = vdC_keys[-1]['Key']
    vdC_dataset = s3.get_object(Bucket=trading_bucket, Key=vdC_key)
    vdC_df = pd.read_csv(vdC_dataset.get("Body"))
    vdC_df['strategy'] = 'vdiff_gainC'
    vdC_df.drop(['Unnamed: 0'],axis=1,inplace=True)

    vdP_keys = s3.list_objects(Bucket=trading_bucket,Prefix="classifier_predictions/invalerts-xgb-vdiff-gainP-classifier/")["Contents"]
    vdP_key = vdP_keys[-1]['Key']
    vdP_dataset = s3.get_object(Bucket=trading_bucket, Key=vdP_key)
    vdP_df = pd.read_csv(vdP_dataset.get("Body"))
    vdP_df['strategy'] = 'vdiff_gainP'
    vdP_df.drop(['Unnamed: 0'],axis=1,inplace=True)


    final_df = pd.concat([gainers_df, losers_df, ma_df, maP_df,vdP_df,vdC_df])
    final_csv = final_df.to_csv().encode()
    query_key = vdP_key.split("classifier_predictions/invalerts-xgb-vdiff-gainP-classifier/vdiff_gain")[1]
    
    try:
        put_response = s3.put_object(Bucket=trading_bucket, Key=f'inv-alerts-full-results{query_key}', Body=final_csv)
        return put_response
    except:
        return False
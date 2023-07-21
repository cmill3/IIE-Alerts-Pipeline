import json
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import boto3
import logging
import requests
from botocore.exceptions import ClientError
import concurrent.futures

alerts_bucket = os.getenv("ALERTS_BUCKET")

index_list = ["SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","VTV","BND","AGG","VUG","VWO","IEMG","IWF","VIG","IJH","IJR","GLD",
    "VGT","VXUS","VO","IWM","BNDX","EFA","IWD","VYM","SCHD","XLK","ITOT","VB","VCIT","XLV","TLT","BSV","VCSH","LQD","XLE","VEU","RSP"]
leveraged_etfs = ["TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
# hours = [10,11,12,13,14,15]
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3', aws_access_key_id="AKIAWUN5YYJZHGIGMLQJ", aws_secret_access_key="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")
logger = logging.getLogger()

def pull_files_s3(s3, bucket, key):
    """
    Pulls files from S3 bucket
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    data = pd.read_csv(response.get("Body"))
    return data['Unnamed: 0'].values[1:]

def generate_volatility_features(row):
    try:
        to_stamp = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
        from_stamp = to_stamp - timedelta(days=145)
        from_str = from_stamp.strftime("%Y-%m-%d")
        to_str = row['date'].split(" ")[0]
        aggs = call_polygon_hist([row['symbol']], from_str, to_str, "day", 1)
        aggs['fiveD_returns_close'] = aggs['c'].pct_change(5)*100
        aggs['threeD_returns_close'] = aggs['c'].pct_change(3)*100
        aggs['oneD_returns_close'] = aggs['c'].pct_change(1)*100
        fiveD_stddev100 = np.std(aggs['fiveD_returns_close'].iloc[-5:])
        fiveD_stddev50 = np.std(aggs['fiveD_returns_close'].iloc[-50:])
        threeD_stddev100 = np.std(aggs['threeD_returns_close'].iloc[-3:])
        threeD_stddev50 = np.std(aggs['threeD_returns_close'].iloc[-50:])
        oneD_stddev100 = np.std(aggs['oneD_returns_close'].iloc[-3:])
        oneD_stddev50 = np.std(aggs['oneD_returns_close'].iloc[-50:])
        return fiveD_stddev100,fiveD_stddev50,threeD_stddev100,threeD_stddev50,oneD_stddev100,oneD_stddev50
    except Exception as e:
        try:
            to_stamp = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
            from_stamp = to_stamp - timedelta(days=145)
            from_str = from_stamp.strftime("%Y-%m-%d")
            to_str = row['date'].split(" ")[0]
            aggs = call_polygon_hist([row['symbol']], from_str, to_str, "day", 1)
            aggs['fiveD_returns_close'] = aggs['c'].pct_change(5)*100
            aggs['threeD_returns_close'] = aggs['c'].pct_change(3)*100
            aggs['oneD_returns_close'] = aggs['c'].pct_change(1)*100
            fiveD_stddev100 = np.std(aggs['fiveD_returns_close'].iloc[-5:])
            fiveD_stddev50 = np.std(aggs['fiveD_returns_close'].iloc[-50:])
            threeD_stddev100 = np.std(aggs['threeD_returns_close'].iloc[-3:])
            threeD_stddev50 = np.std(aggs['threeD_returns_close'].iloc[-50:])
            oneD_stddev100 = np.std(aggs['oneD_returns_close'].iloc[-3:])
            oneD_stddev50 = np.std(aggs['oneD_returns_close'].iloc[-50:])
            return fiveD_stddev100,fiveD_stddev50,threeD_stddev100,threeD_stddev50,oneD_stddev100,oneD_stddev50
        except Exception as e:
            print(e)
            print(row['symbol'])
            return 0,0,0,0,0,0


def build_relative_volatility_features(date_str):
    print(date_str)
    # date_str = event["date"]
    hours = [10,11,12,13,14,15]
    key_str = date_str.replace("-","/")
    for hour in hours:
        try:
            get_response = s3.get_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/all_alerts/{key_str}/{hour}.csv")
            data = pd.read_csv(get_response.get("Body"))
            result = data.apply(generate_volatility_features, axis=1)
            result = pd.DataFrame(result.to_list())
            result.columns = ["fiveD_stddev100","fiveD_stddev50","threeD_stddev100","threeD_stddev50","oneD_stddev100","oneD_stddev50"]
            final = data.join([result])
            alerts_dict = build_alerts(final)
            for key, df in alerts_dict.items():
                try:
                    csv = df.to_csv()
                    put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/stddev/{key}/{key_str}/{hour}.csv", Body=csv)
                except ClientError as e:
                    logging.error(f"error for {key} :{e})")
                    continue
            # put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/stddev/{key}/{key_str}/{hour}.csv", Body=csv)
        except ClientError as e:
            logging.error(f"error for {key} :{e})")
            continue

def call_polygon_hist(symbol, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    error_list = []
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol[0]}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"

    response = requests.request("GET", url, headers=headers, data=payload)

    response_data = json.loads(response.text)
    results = response_data['results']
    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['day'] = results_df['date'].apply(lambda x: x.day)
    # results_df['symbol'] = row['symbol']
    # dfs.append(results_df)

    return results_df

def build_alerts(alerts):
    c_sorted = alerts.sort_values(by="close_diff", ascending=False)
    v_sorted = alerts.sort_values(by="v", ascending=False)
    vdiff_sorted = alerts.sort_values(by="v_diff_pct", ascending=False)
    gainers = c_sorted.head(50)
    losers = c_sorted.tail(50)
    gt = c_sorted.loc[c_sorted["close_diff"] > 0.025]
    lt = c_sorted.loc[c_sorted["close_diff"] < -0.025]
    most_active = v_sorted.head(50)
    volume_gain = vdiff_sorted.head(50)
    volume_loss = vdiff_sorted.tail(50)
    return {"all_alerts":alerts, "gainers": gainers, "losers": losers, "gt":gt, "lt":lt, "most_actives":most_active, "vdiff_gain":volume_gain, "vdiff_loss":volume_loss}

# def backtest_orchestrator(date_list):
#     with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
#         # Submit the processing tasks to the ThreadPoolExecutor
#         processed_weeks_futures = [executor.submit(build_relative_volatility_features, date_str) for date_str in date_list]


if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2021,6,3)
    end_date = datetime(2022,7,1)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(build_relative_volatility_features, date_str) for date_str in date_list]
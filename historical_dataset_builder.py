import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import *
from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError
import concurrent.futures

alerts_bucket = os.getenv("ALERTS_BUCKET")

indexes = ['QQQ','SPY','IWM']
memes = ['GME','AMC','MARA','TSLA','BBY','NIO','RIVN','XPEV','COIN','ROKU','LCID']
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3')
logger = logging.getLogger()


def run_process(date_str):
    try:
        build_historic_data(date_str)
    except Exception as e:
        print(e)
        build_historic_data(date_str)
    print(f"Finished {date_str}")

def build_historic_data(date_str):
    print(date_str)
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    year, month, day = from_stamp.split("-")
    if datetime(int(year),int(month),int(day)) <= datetime(2022,6,1):
        big_fish =  [
            "AMD","NVDA","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP","FB","CRM",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","QQQ",'SPY','IWM'
            ]
    else:
       big_fish =  [
            "AMD","NVDA","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP","CRM",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","META","QQQ",'SPY','IWM'
            ] 
    for hour in hours:
        aggregates, error_list = call_polygon_histD(big_fish, from_stamp, to_stamp, timespan="minute", multiplier="30")
        hour_aggregates, error_list = call_polygon_histH(big_fish, hour_stamp, hour_stamp, timespan="minute", multiplier="30")
        full_aggs = combine_hour_aggs(aggregates, hour_aggregates, hour)
        df = build_analytics(full_aggs, hour)
        df.reset_index(drop=True, inplace=True)
        df = df.groupby("symbol").tail(1)
        spy_aggs = call_polygon_spy(from_stamp, to_stamp, timespan="minute", multiplier="30")
        current_spy = call_polygon_spyH(hour_stamp, hour_stamp, timespan="hour", multiplier="1", hour=int(hour))
        current_spy = current_spy.values[0]
        result = df.apply(calc_price_action, axis=1)
        result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
        result.reset_index()
        price = pd.DataFrame(result.to_list())
        df.reset_index(drop=True, inplace=True)
        df['one_max'] = price['one_max']
        df['one_min'] = price['one_min']
        df['one_pct'] = price['one_pct']
        df['three_max'] = price['three_max']
        df['three_min'] = price['three_min']
        df['three_pct'] = price['three_pct']
        SPY_diff   = (current_spy - spy_aggs[-1])/spy_aggs[-1]
        SPY_diff3  = (current_spy - spy_aggs[-3])/spy_aggs[-3]
        SPY_diff5  = (current_spy - spy_aggs[-5])/spy_aggs[-5]
        df['SPY_diff'] = (((df['close_diff']/100) - SPY_diff)/SPY_diff)
        df['SPY_diff3'] = (((df['close_diff']/100) - SPY_diff3)/SPY_diff3)
        df['SPY_diff5'] = (((df['close_diff']/100) - SPY_diff5)/SPY_diff5)
        df['SPY_1D'] = SPY_diff
        df['SPY_3D'] = SPY_diff3
        df['SPY_5D'] = SPY_diff5
        csv = df.to_csv()
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_mktHours/{key_str}/{hour}.csv", Body=csv)
    return put_response
    
def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=8)
    end_day = end - timedelta(days=1)
    to_stamp = end_day.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp


def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    hour_df = pd.concat(hour_aggregates)
    for index, value in enumerate(aggregates):
        hour_aggs = hour_df.loc[hour_df["symbol"] == value.iloc[0]['symbol']]
        hour_aggs = hour_aggs.loc[hour_aggs["hour"] < int(hour)]
        if len(hour_aggs) > 1:
            hour_aggs = hour_aggs.iloc[:-1]
        volume = hour_aggs.v.sum()
        open = hour_aggs.o.iloc[0]
        close = hour_aggs.c.iloc[-1]
        high = hour_aggs.h.max()
        low = hour_aggs.l.min()
        n = hour_aggs.n.sum()
        t = hour_aggs.t.iloc[-1]
        aggs_list = [volume, open, close, high, low, hour_aggs.date.iloc[-1], hour,hour_aggs.symbol.iloc[-1],t]
        value.loc[len(value)] = aggs_list
        full_aggs.append(value)
    return full_aggs




def pull_df(date_stamp, prefix, hour):
    date_stamp = date_stamp.replace("-","/")
    try: 
        dataset = s3.get_object(Bucket="inv-alerts", Key=f"{prefix}{date_stamp}/{hour}.csv")
        df = pd.read_csv(dataset.get("Body"))
    except Exception as e:
        print(f"{prefix}{date_stamp}/{hour}.csv")
        print(f"HERE {e}")
    return df


if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2022,2,19)
    end_date = datetime(2022,4,1)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    # run_process("2022-01-06")

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]

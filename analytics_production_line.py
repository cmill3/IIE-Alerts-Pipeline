import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import *
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")
big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","SPY","CRM"
            ,"QQQ","IWM"]


now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
logger = logging.getLogger()

def analytics_runner(event, context):
    s3 = get_s3_client()
    date = datetime.now()
    year, month, day, hour = format_dates(date)
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date)
    aggregates, error_list = call_polygon_histD(big_fish, from_stamp, to_stamp, timespan="minute", multiplier="30")
    hour_aggregates, error_list = call_polygon_histH(big_fish, hour_stamp, hour_stamp, timespan="minute", multiplier="30")
    full_aggs = combine_hour_aggs(aggregates, hour_aggregates, hour)
    spy_aggs = call_polygon_spy(from_stamp, to_stamp, timespan="day", multiplier="1")
    logger.info(f"Error list: {error_list}")
    analytics = build_analytics(full_aggs, hour)
    analytics = build_spy_features(analytics, spy_aggs)
    df = analytics.groupby("symbol").tail(1)
    min_aggs, error_list = call_polygon_vol(df['symbol'], from_stamp, to_stamp, timespan="minute", multiplier="1", hour=hour)
    hour_aggs, error_list = call_polygon_vol(df['symbol'], from_stamp, to_stamp, timespan="minute", multiplier="30", hour=hour)
    df = vol_feature_engineering(df, min_aggs, hour_aggs)
    df['hour'] = hour
    csv = df.to_csv()
    put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/test/{year}/{month}/{day}/{hour}.csv", Body=csv)
    return put_response

def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    for index, value in enumerate(aggregates):
        hour_aggs = hour_aggregates[index]
        print(hour_aggs)
        print(hour)
        hour_aggs = hour_aggs.loc[hour_aggs["hour"] < int(hour)]
        print(hour_aggs)
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


def generate_dates_historic(now):
    start = now - timedelta(weeks=8)
    end_day = now - timedelta(days=1)
    to_stamp = end_day.strftime("%Y-%m-%d")
    hour_stamp = now.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

def build_full_df(aggregates, day_aggregates, to_stamp):
    symbols_list = aggregates["symbol"].unique()
    temp_dfs = []
    for symbol in symbols_list:
        df = aggregates.loc[aggregates["symbol"] == symbol]
        if to_stamp in df['date']:
            temp_dfs.append(df)
        else:
            day_df = day_aggregates.loc[day_aggregates["symbol"] == symbol]
            v = day_df["v"].sum()
            c = day_df["c"].iloc[-1]
            h = day_df["h"].max()
            l = day_df["l"].min()
            t = day_df["t"].iloc[-1]
            vw = day_df["vw"].mean()
            n = day_df["n"].sum()
            df.append({"v":v, "c":c,"h":h,"l":l,"t":t,"vw":vw,"n":n}, ignore_index=True, inplace=True)
            temp_dfs.append(df)
    full_df = pd.concat(temp_dfs)
    return full_df

def format_dates(now):
    now_str = now.strftime("%Y-%m-%d-%H")
    year, month, day, hour = now_str.split("-")
    hour = int(hour)
    return year, month, day, hour

if __name__ == "__main__":
    start_date = datetime.now()
    print(start_date)
    analytics_runner(None,None)
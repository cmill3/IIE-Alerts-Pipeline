import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import call_polygon, build_analytics, call_polygon_spy, build_spy_features
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")
big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","F","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","SNOW","SOFI","SPY"
            ,"QQQ","IWM"
            ]


now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
logger = logging.getLogger()

def analytics_runner(event, context):
    s3 = get_s3_client()
    date = datetime.now()
    year, month, day, hour = format_dates(date)
    from_stamp, to_stamp = generate_dates(date)
    aggregates, error_list = call_polygon(big_fish, from_stamp, to_stamp, timespan="day", multiplier="1")
    spy_aggs = call_polygon_spy(from_stamp, to_stamp, timespan="day", multiplier="1")
    logger.info(f"Error list: {error_list}")
    analytics = build_analytics(aggregates, hour)
    analytics = build_spy_features(analytics, spy_aggs)
    alerts = analytics.groupby("symbol").tail(1)
    alerts['hour'] = hour
    csv = alerts.to_csv()
    put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/{year}/{month}/{day}/{hour}.csv", Body=csv)
    return put_response

def generate_dates(now):
    start = now - timedelta(weeks=6)
    to_stamp = now.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp

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
    hour = int(hour) - 4
    return year, month, day, hour

if __name__ == "__main__":
    start_date = datetime.now()
    print(start_date)
    analytics_runner(None,None)
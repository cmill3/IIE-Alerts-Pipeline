import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import call_polygon, build_analytics, get_pcr
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")

index_list = ["SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","VTV","BND","AGG","VUG","VWO","IEMG","IWF","VIG","IJH","IJR","GLD",
    "VGT","VXUS","VO","IWM","BNDX","EFA","IWD","VYM","SCHD","XLK","ITOT","VB","VCIT","XLV","TLT","BSV","VCSH","LQD","XLE","VEU","RSP"]
leveraged_etfs = ["TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")

logger = logging.getLogger()

def analytics_runner(event, context, date):
    s3 = get_s3_client()
    sp_500 = pull_files_s3(s3, "icarus-research-data", "index_lists/S&P500.csv")
    full_list = index_list + leveraged_etfs + sp_500.tolist()
    from_stamp, to_stamp = generate_dates(date)
    hour = date.hour
    aggregates, error_list = call_polygon(full_list, from_stamp, to_stamp, timespan="day", multiplier="1")
    logger.info(f"Error list: {error_list}")
    analytics = build_analytics(aggregates, get_pcr, hour)
    alerts_dict = build_alerts(analytics)
    for key, value in alerts_dict.items():
        try:
            csv = value.to_csv()
            put_response = s3.put_object(Bucket=alerts_bucket, Key=f"{key}/{now_str}.csv", Body=csv)
        except ClientError as e:
            logging.error(f"error for {key} :{e})")
            continue
    return put_response

def generate_dates(date):
    now = date
    # now = datetime.now()
    start = now - timedelta(weeks=6)
    to_stamp = now.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp

def build_alerts(df):
    alerts = df.groupby("symbol").tail(1)
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
    return {"all_alerts":alerts,"gainers": gainers, "losers": losers, "gt":gt, "lt":lt, "most_actives":most_active, "vdiff_gain":volume_gain, "vdiff_loss":volume_loss}

def build_full_df(aggregates, day_aggregates, to_stamp):
    print(aggregates)
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


if __name__ == "__main__":
    start_date = datetime.now()
    print(start_date)
    analytics_runner(None,None,start_date)
    # end_date = datetime(2023,6,16)
    # date_diff = end_date - start_date
    # numdays = date_diff.days 
    # date_list = []
    # print(numdays)
    # for x in range (0, numdays):
    #     temp_date = start_date + timedelta(days = x)
    #     if temp_date.weekday() < 5:
    #         # date_str = temp_date.strftime("%Y/%m/%d")
    #         date_list.append(temp_date)

    # for date in date_list:
    #     analytics_runner(None,None,date)
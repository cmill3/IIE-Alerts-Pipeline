import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import call_polygon, build_analytics, get_pcr_historic
from datetime import datetime, timedelta
import os
import logging
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")

index_list = ["SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","VTV","BND","AGG","VUG","VWO","IEMG","IWF","VIG","IJH","IJR","GLD",
    "VGT","VXUS","VO","IWM","BNDX","EFA","IWD","VYM","SCHD","XLK","ITOT","VB","VCIT","XLV","TLT","BSV","VCSH","LQD","XLE","VEU","RSP"]
leveraged_etfs = ["TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")

logger = logging.getLogger()

def build_historic_data(event, context):
    date_str = event["date"]
    s3 = get_s3_client()
    sp_500 = pull_files_s3(s3, "icarus-research-data", "index_lists/S&P500.csv")
    full_list = index_list + leveraged_etfs + sp_500.tolist()
    from_stamp, to_stamp = generate_dates_historic(date_str)
    aggregates, error_list = call_polygon(full_list, from_stamp, to_stamp, timespan="day", multiplier="1")
    logger.info(f"Error list: {error_list}")
    analytics = build_analytics(aggregates, get_pcr_historic)
    alerts_dict = build_alerts(analytics)
    for key, value in alerts_dict.items():
        try:
            csv = value.to_csv()
            put_response = s3.put_object(Bucket=alerts_bucket, Key=f"inv_alerts/{key}/{now_str}.csv", Body=csv)
        except ClientError as e:
            logging.error(f"error for {key} :{e})")
            continue
    return put_response

def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=6)
    to_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp

def build_alerts(df):
    alerts = df.groupby("symbol").tail(1)
    c_sorted = alerts.sort_values(by="close_diff", ascending=False)
    v_sorted = alerts.sort_values(by="v", ascending=False)
    vdiff_sorted = alerts.sort_values(by="volume_diff", ascending=False)
    gainers = c_sorted.head(50)
    losers = c_sorted.tail(50)
    gt = c_sorted.loc[c_sorted["close_diff"] > 0.025]
    lt = c_sorted.loc[c_sorted["close_diff"] < -0.025]
    most_active = v_sorted.head(50)
    volume_diff = vdiff_sorted.head(50)
    return {"all_alerts":alerts,"gainers": gainers, "losers": losers, "gt":gt, "lt":lt, "most_actives":most_active, "vdiff":volume_diff}


if __name__ == "__main__":
    build_historic_data("event", "context")
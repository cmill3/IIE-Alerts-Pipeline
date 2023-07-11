import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import call_polygon, build_analytics, get_pcr_historic, calc_price_action, calc_vdiff
from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")

index_list = ["SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","VTV","BND","AGG","VUG","VWO","IEMG","IWF","VIG","IJH","IJR","GLD",
    "VGT","VXUS","VO","IWM","BNDX","EFA","IWD","VYM","SCHD","XLK","ITOT","VB","VCIT","XLV","TLT","BSV","VCSH","LQD","XLE","VEU","RSP"]
leveraged_etfs = ["TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
# hours = [10,11,12,13,14,15]
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3')
logger = logging.getLogger()

# def build_historic_data(event, context):
#     date_str = event["date"]
#     hour = event["hour"]
#     # date_str = "2022-07-29"
#     key_str = date_str.replace("-","/")
#     s3 = get_s3_client()
#     sp_500 = pull_files_s3(s3, "icarus-research-data", "index_lists/S&P500.csv")
#     full_list = index_list + leveraged_etfs + sp_500.tolist()
#     from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
#     try:
#         aggregates, error_list = call_polygon(full_list, from_stamp, to_stamp, timespan="day", multiplier="1")
#         hour_aggregates, err = call_polygon(full_list, hour_stamp, hour_stamp, timespan="hour", multiplier="1")
#         hour_df = pd.concat(hour_aggregates)
#         logger.info(f"Error list: {error_list}")
#     except Exception as e: 
#         return "no records"
#     # for hour in hours:
#     analytics = build_analytics(aggregates, hour_df, hour, get_pcr_historic)
#     alerts_dict = build_alerts(analytics)
#     for key, value in alerts_dict.items():
#         try:
#             csv = value.to_csv()
#             put_response = s3.put_object(Bucket=alerts_bucket, Key=f"inv_alerts/{key}/{key_str}/{hour}.csv", Body=csv)
#         except ClientError as e:
#             logging.error(f"error for {key} :{e})")
#             continue
#     return put_response

def run(date_stamp):
    # date_stamp = event['date']
    hours = ["10","11","12","13","14","15"]
    # date_stamp = "2022/07/27"
    
    # gainers_df = helpers.build_unprocessed_df(date_stamp,"expanded_alert_values/day_gainers/")
    # losers_df = helpers.build_unprocessed_df(date_stamp,"expanded_alert_values/day_losers/")
    # ma_df = helpers.build_unprocessed_df(date_stamp,"expanded_alert_values/most_actives/")
    for hour in hours:
        try:
            gt_df = pull_df(date_stamp,"inv_alerts_with_price/gt/",hour)
            lt_df = pull_df(date_stamp,"inv_alerts_with_price/lt/",hour)
            gainers_df = pull_df(date_stamp,"inv_alerts_with_price/gainers/",hour)
            losers_df = pull_df(date_stamp,"inv_alerts_with_price/losers/",hour)
            ma_df = pull_df(date_stamp,"inv_alerts_with_price/most_actives/",hour)
            vdiff_df = pull_df(date_stamp,"inv_alerts_with_price/vdiff/",hour)
            dfs = {"gt":gt_df,"lt":lt_df,"gainers":gainers_df,"losers":losers_df,"vdiff":vdiff_df,"most_actives":ma_df}
        except:
            return "NO DATA"
    
        for key, df in dfs.items():
            try:
                # df[['one_max','one_min','one_pct','three_max','three_min','three_pct']] = df.apply(calc_price_action, axis=1)
                df['v_diff_pct'] = df.apply(calc_vdiff, axis=1).apply(pd.Series)
                # result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
                # df = pd.concat([df, result], axis=1)
                csv = df.to_csv()
                put_response = s3.put_object(Bucket=alerts_bucket, Key=f"inv_alerts_with_price/{key}/{date_stamp}/{hour}.csv", Body=csv)
            except ClientError as e:
                logging.error(f"error for {key} :{e})")
                print(f"error for {key} :{e})")
                continue
    
    return put_response

def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=6)
    end_day = end - timedelta(days=1)
    to_stamp = end_day.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

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

def pull_df(date_stamp, prefix, hour):
    try: 
        dataset = s3.get_object(Bucket=alerts_bucket, Key=f"{prefix}{date_stamp}/{hour}.csv")
        df = pd.read_csv(dataset.get("Body"))
    except Exception as e:
        print(f"HERE {e}")
    return df


if __name__ == "__main__":
    start_date = datetime(2022,5,27)
    end_date = datetime(2022,7,1)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y/%m/%d")
            date_list.append(date_str)

    for date_str in date_list:
        print(date_str)
        run(date_str)
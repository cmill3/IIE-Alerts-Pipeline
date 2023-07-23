import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import call_polygon_hist, build_analytics, get_pcr_historic, calc_price_action, calc_vdiff
from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError
import concurrent.futures

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

# def run(date_stamp):
#     # date_stamp = event['date']
#     hours = ["10","11","12","13","14","15"]
#     # date_stamp = "2022/07/27"
    
#     # gainers_df = helpers.build_unprocessed_df(date_stamp,"expanded_alert_values/day_gainers/")
#     # losers_df = helpers.build_unprocessed_df(date_stamp,"expanded_alert_values/day_losers/")
#     # ma_df = helpers.build_unprocessed_df(date_stamp,"expanded_alert_values/most_actives/")
#     for hour in hours:
#         try:
#             gt_df = pull_df(date_stamp,"inv_alerts_with_price/gt/",hour)
#             lt_df = pull_df(date_stamp,"inv_alerts_with_price/lt/",hour)
#             gainers_df = pull_df(date_stamp,"inv_alerts_with_price/gainers/",hour)
#             losers_df = pull_df(date_stamp,"inv_alerts_with_price/losers/",hour)
#             ma_df = pull_df(date_stamp,"inv_alerts_with_price/most_actives/",hour)
#             vdiff_df = pull_df(date_stamp,"inv_alerts_with_price/vdiff/",hour)
#             dfs = {"gt":gt_df,"lt":lt_df,"gainers":gainers_df,"losers":losers_df,"vdiff":vdiff_df,"most_actives":ma_df}
#         except:
#             return "NO DATA"
    
#         for key, df in dfs.items():
#             try:
#                 # df[['one_max','one_min','one_pct','three_max','three_min','three_pct']] = df.apply(calc_price_action, axis=1)
#                 df['v_diff_pct'] = df.apply(calc_vdiff, axis=1).apply(pd.Series)
#                 # result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
#                 # df = pd.concat([df, result], axis=1)
#                 csv = df.to_csv()
#                 put_response = s3.put_object(Bucket=alerts_bucket, Key=f"inv_alerts_with_price/{key}/{date_stamp}/{hour}.csv", Body=csv)
#             except ClientError as e:
#                 logging.error(f"error for {key} :{e})")
#                 print(f"error for {key} :{e})")
#                 continue
    
#     return put_response

def build_historic_data(date_str):
    print(date_str)
    hours = ["10","11","12","13","14","15"]
    # distibuted_number = 1
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    sp_500 = pull_files_s3(s3, "icarus-research-data", "index_lists/S&P500.csv")
    full_list = index_list + sp_500.tolist()
    from_stamp, to_stamp, _ = generate_dates_historic(date_str)
    for hour in hours:
        aggregates, error_list = call_polygon_hist(full_list, from_stamp, to_stamp, timespan="day", multiplier="1",hour=hour)
        logger.info(f"Error list: {error_list}")
        analytics = build_analytics(aggregates, hour)
        alerts_dict = build_alerts(analytics)
        for key, df in alerts_dict.items():
            csv = df.to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"fixed_alerts/{key}/{key_str}/{hour}.csv", Body=csv)
    # for key, df in alerts_dict.items():
    #     try:
    #         csv = df.to_csv()
    #         put_response = s3.put_object(Bucket="inv-alerts", Key=f"{key}/{key_str}/{hour}.csv", Body=csv)
    #         put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/{key}/{key_str}/{hour}.csv", Body=csv)
    #     except ClientError as e:
    #         logging.error(f"error for {key} :{e})")
    #         continue
        # try:
        #     # df[['one_max','one_min','one_pct','three_max','three_min','three_pct']] = df.apply(calc_price_action, axis=1)
        #     result = df.apply(calc_price_action, axis=1)
        #     result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
        #     result.reset_index()
        #     price = pd.DataFrame(result.to_list())
        #     df.reset_index(drop=True, inplace=True)
        #     df['one_max'] = price['one_max']
        #     df['one_min'] = price['one_min']
        #     df['one_pct'] = price['one_pct']
        #     df['three_max'] = price['three_max']
        #     df['three_min'] = price['three_min']
        #     df['three_pct'] = price['three_pct']
        #     print(df)
        #     # df = pd.concat([df, price],axis=1)
        #     csv = df.to_csv()
        #     put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/{key}/{key_str}/{hour}.csv", Body=csv)
        # except ClientError as e:
        #     logging.error(f"error for {key} :{e})")
        #     print(f"error for {key} :{e})")
        #     continue
    return put_response
    
def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=6)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

def build_price_action(df):
    alerts = df.groupby("symbol").tail(1)
    alerts.reset_index(drop=True, inplace=True)
    result = alerts.apply(calc_price_action, axis=1)
    result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
    result.reset_index()
    price = pd.DataFrame(result.to_list())
    alerts['one_max'] = price['one_max']
    alerts['one_min'] = price['one_min']
    alerts['one_pct'] = price['one_pct']
    alerts['three_max'] = price['three_max']
    alerts['three_min'] = price['three_min']
    alerts['three_pct'] = price['three_pct']
    return alerts

def build_alerts(df):
    alerts = df.groupby("symbol").tail(1)
    alerts.reset_index(drop=True, inplace=True)
    result = alerts.apply(calc_price_action, axis=1)
    result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
    final = alerts.join([result])
    # alerts['one_max'] = price['one_max']
    # alerts['one_min'] = price['one_min']
    # alerts['one_pct'] = price['one_pct']
    # alerts['three_max'] = price['three_max']
    # alerts['three_min'] = price['three_min']
    # alerts['three_pct'] = price['three_pct']
    c_sorted = final.sort_values(by="close_diff", ascending=False)
    v_sorted = final.sort_values(by="v", ascending=False)
    vdiff_sorted = final.sort_values(by="v_diff_pct", ascending=False)
    gainers = c_sorted.head(50)
    losers = c_sorted.tail(50)
    # gt = c_sorted.loc[c_sorted["close_diff"] > 0.025]
    # lt = c_sorted.loc[c_sorted["close_diff"] < -0.025]
    most_active = v_sorted.head(50)
    volume_gain = vdiff_sorted.head(50)
    return {"all_alerts":final, "gainers": gainers, "losers": losers, "most_actives":most_active, "vdiff_gain":volume_gain}

def pull_df(date_stamp, prefix, hour):
    try: 
        dataset = s3.get_object(Bucket=alerts_bucket, Key=f"{prefix}{date_stamp}/{hour}.csv")
        df = pd.read_csv(dataset.get("Body"))
    except Exception as e:
        print(f"HERE {e}")
    return df


if __name__ == "__main__":
    # build_historic_data(None, None)
    start_date = datetime(2022,1,1)
    end_date = datetime(2023,7,14)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    # for date_str in date_list:
    #     build_historic_data(date_str)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(build_historic_data, date_str) for date_str in date_list]
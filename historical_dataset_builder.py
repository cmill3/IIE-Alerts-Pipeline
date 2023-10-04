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

# weekly_expiries = ['SPY', 'IVV', 'QQQ', 'GLD', 'IWM', 'EFA', 'XLK', 'XLV', 'TLT', 'LQD', 'XLE', 'TQQQ', 'SQQQ', 'SPXS', 'SPXL', 'SOXL', 'SOXS', 'MMM', 'ABT', 'ABBV', 'ACN', 'ATVI', 'ADM', 'ADBE', 'ADP', 
#                    'AAP', 'AFL', 'ALB', 'ALGN', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMD', 'AAL', 'AXP', 'AIG', 'ABC', 'AMGN', 'ADI', 'APA', 'AAPL', 'AMAT', 'ANET', 'T', 'ADSK', 'BAC', 'BBWI', 'BAX', 'BBY', 'BIIB', 
#                    'BLK', 'BA', 'BKNG', 'BMY', 'AVGO', 'CZR', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CAT', 'CBOE', 'CNC', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CI', 'CSCO', 'C', 'CLX', 'CME', 'KO', 'CMCSA', 'CMA', 'CAG', 
#                    'COP', 'STZ', 'GLW', 'COST', 'CTRA', 'CSX', 'CVS', 'DHI', 'DHR', 'DE', 'DAL', 'DVN', 'DLR', 'DFS', 'DISH', 'DIS', 'DG', 'DLTR', 'DPZ', 'DOW', 'DD', 'EBAY', 'EA', 'ELV', 'LLY', 'EMR', 'ENPH', 'EOG', 'EQT', 
#                    'ETSY', 'EXPE', 'XOM', 'FDX', 'FITB', 'FSLR', 'FI', 'F', 'FTNT', 'FOXA', 'FCX', 'GEHC', 'GNRC', 'GD', 'GE', 'GM', 'GILD', 'GS', 'HAL', 'HSY', 'HES', 'HD', 'HON', 'HRL', 'HPQ', 'HUM', 'HBAN', 'IBM', 'ILMN', 
#                    'INTC', 'IP', 'INTU', 'ISRG', 'JNJ', 'JPM', 'JNPR', 'KEY', 'KMB', 'KMI', 'KLAC', 'KHC', 'KR', 'LRCX', 'LVS', 'LEN', 'LMT', 'LOW', 'MRO', 'MPC', 'MAR', 'MA', 'MTCH', 'MCD', 'MCK', 'MDT', 'MRK', 'META', 'MET', 
#                    'MGM', 'MU', 'MSFT', 'MRNA', 'MDLZ', 'MS', 'MOS', 'NTAP', 'NFLX', 'NEM', 'NKE', 'NSC', 'NOC', 'NCLH', 'NUE', 'NVDA', 'NXPI', 'OXY', 'ON', 'ORCL', 'PARA', 'PYPL', 'PEP', 'PFE', 'PCG', 'PM', 'PSX', 'PXD', 'PNC', 
#                    'PPG', 'PG', 'PHM', 'QCOM', 'RTX', 'REGN', 'ROST', 'RCL', 'SPGI', 'CRM', 'SLB', 'STX', 'NOW', 'SWKS', 'SEDG', 'SO', 'LUV', 'SBUX', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TSLA', 'TXN', 'TMO', 'TJX', 'TSCO', 'TFC', 
#                    'TSN', 'USB', 'ULTA', 'UNP', 'UAL', 'UPS', 'URI', 'UNH', 'VLO', 'VZ', 'VRTX', 'VFC', 'V', 'WBA', 'WMT', 'WBD', 'WM', 'WFC', 'WDC', 'WHR', 'WMB', 'WYNN', 'ZION']
# hours = [10,11,12,13,14,15]
# index_list = ["SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","VTV","BND","AGG","VUG","VWO","IEMG","IWF","VIG","IJH","IJR","GLD",
#     "VGT","VXUS","VO","IWM","BNDX","EFA","IWD","VYM","SCHD","XLK","ITOT","VB","VCIT","XLV","TLT","BSV","VCSH","LQD","XLE","VEU","RSP"]
# leveraged_etfs = ["TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
big_fish =  [
            "AMD","NVDA","META","PYPL","GOOG","GOOGL","AMZN","PLTR","BAC","AAPL","NFLX","ABNB","CRWD","SHOP",
            "MSFT","FB","V","MA","JNJ","DIS","JPM","INTC","ADBE","BA","CVX","MRNA","PFE","FB","SNOW","SOFI",
            ]
indexes = ['QQQ','SPY','IWM']
memes = ['GME','AMC','MARA','TSLA','BBY','NIO','RIVN','XPEV','COIN','ROKU','LCID']
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
#     from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_stamp)
#     print(from_stamp, to_stamp, hour_stamp)
#     key_stamp = date_stamp.replace("-","/")
#     for hour in hours:
#         try:
#             # gt_df = pull_df(date_stamp,"inv_alerts_with_price/gt/",hour)
#             # lt_df = pull_df(date_stamp,"inv_alerts_with_price/lt/",hour)
#             gainers_df = pull_df(hour_stamp,"gainers/",hour)
#             losers_df = pull_df(hour_stamp,"losers/",hour)
#             ma_df = pull_df(hour_stamp,"most_actives/",hour)
#             vdiff_df = pull_df(hour_stamp,"vdiff_gain/",hour)
#             dfs = {"gainers":gainers_df,"losers":losers_df,"vdiff":vdiff_df,"most_actives":ma_df}
#         except:
#             return "NO DATA"
    
#         for key, df in dfs.items():
#             try:
#                 df = df.loc[df['symbol']!="RE"]
#                 symbol_list = df['symbol'].to_list()
#                 aggregates, error_list = call_polygon_hist(symbol_list, from_stamp, to_stamp, timespan="day", multiplier="1")
#                 hour_aggregates, error_list = call_polygon_hist(symbol_list, hour_stamp, hour_stamp, timespan="hour", multiplier="1")
#                 print(len(hour_aggregates))
#                 print(len(aggregates))
#                 full_aggs = combine_hour_aggs(aggregates, hour_aggregates, hour)
#                 new_price_features = build_new_price_features(full_aggs)
#                 df = pd.merge(df, new_price_features[['rsi3','rsi5','close_diff3','close_diff5','symbol']], on="symbol")
#                 df['hour'] = hour
#                 # df[['one_max','one_min','one_pct','three_max','three_min','three_pct']] = df.apply(calc_price_action, axis=1)
#                 result = df.apply(calc_price_action, axis=1)
#                 # df['v_diff_pct'] = df.apply(calc_vdiff, axis=1).apply(pd.Series)
#                 spy_aggs = call_polygon_spy(from_stamp, to_stamp, timespan="day", multiplier="1")
#                 current_spy = call_polygon_spyH(hour_stamp, hour_stamp, timespan="hour", multiplier="1", hour=hour)
#                 current_spy = current_spy.values[0]
#                 SPY_diff   = (current_spy - spy_aggs[-1])/spy_aggs[-1]
#                 SPY_diff3  = (current_spy - spy_aggs[-3])/spy_aggs[-3]
#                 SPY_diff5  = (current_spy - spy_aggs[-5])/spy_aggs[-5]
#                 df['SPY_diff'] = (((df['close_diff']/100) - SPY_diff)/SPY_diff)
#                 df['SPY_diff3'] = (((df['close_diff']/100) - SPY_diff3)/SPY_diff3)
#                 df['SPY_diff5'] = (((df['close_diff']/100) - SPY_diff5)/SPY_diff5)
#                 df['SPY_1D'] = SPY_diff
#                 df['SPY_3D'] = SPY_diff3
#                 df['SPY_5D'] = SPY_diff5
#                 price_df = pd.DataFrame(result.to_list())
#                 df = pd.merge(df, price_df, on="symbol")
#                 csv = df.to_csv()
#                 put_response = s3.put_object(Bucket="inv-alerts", Key=f"inv_alerts_training_data/{key}/{key_stamp}/{hour}.csv", Body=csv)
#             except ClientError as e:
#                 logging.error(f"error for {key} :{e})")
#                 print(f"error for {key} :{e})")
#                 continue
    
#     return put_response

# def build_historic_data(date_str):
#     print(date_str)
#     hours = ["10","11","12","13","14","15"]
#     # distibuted_number = 1
#     key_str = date_str.replace("-","/")
#     s3 = get_s3_client()
#     sp_500 = pull_files_s3(s3, "icarus-research-data", "index_lists/S&P500.csv")
#     full_list = index_list + sp_500.tolist()
#     from_stamp, to_stamp, _ = generate_dates_historic(date_str)
#     for hour in hours:
#         aggregates, error_list = call_polygon_hist(full_list, from_stamp, to_stamp, timespan="day", multiplier="1",hour=hour)
#         print(f"Error list: {error_list}")
#         analytics = build_analytics(aggregates, hour)
#         alerts_dict = build_alerts(analytics)
#         for key, df in alerts_dict.items():
#             csv = df.to_csv()
#             put_response = s3.put_object(Bucket="inv-alerts", Key=f"fixed_alerts/{key}/{key_str}/{hour}.csv", Body=csv)
#     # for key, df in alerts_dict.items():
#     #     try:
#     #         csv = df.to_csv()
#     #         put_response = s3.put_object(Bucket="inv-alerts", Key=f"{key}/{key_str}/{hour}.csv", Body=csv)
#     #         put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/{key}/{key_str}/{hour}.csv", Body=csv)
#     #     except ClientError as e:
#     #         logging.error(f"error for {key} :{e})")
#     #         continue
#         # try:
#         #     # df[['one_max','one_min','one_pct','three_max','three_min','three_pct']] = df.apply(calc_price_action, axis=1)
#         #     result = df.apply(calc_price_action, axis=1)
#         #     result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
#         #     result.reset_index()
#         #     price = pd.DataFrame(result.to_list())
#         #     df.reset_index(drop=True, inplace=True)
#         #     df['one_max'] = price['one_max']
#         #     df['one_min'] = price['one_min']
#         #     df['one_pct'] = price['one_pct']
#         #     df['three_max'] = price['three_max']
#         #     df['three_min'] = price['three_min']
#         #     df['three_pct'] = price['three_pct']
#         #     print(df)
#         #     # df = pd.concat([df, price],axis=1)
#         #     csv = df.to_csv()
#         #     put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/{key}/{key_str}/{hour}.csv", Body=csv)
#         # except ClientError as e:
#         #     logging.error(f"error for {key} :{e})")
#         #     print(f"error for {key} :{e})")
#         #     continue
#     return put_response

def build_historic_data(date_str):
    print(date_str)
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = get_s3_client()
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        # df = pull_df(key_str,"fixed_alerts_full/",hour)
        aggregates, error_list = call_polygon_hist(big_fish, from_stamp, to_stamp, timespan="day", multiplier="1")
        hour_aggregates, error_list = call_polygon_hist(big_fish, hour_stamp, hour_stamp, timespan="hour", multiplier="1")
        full_aggs = combine_hour_aggs(aggregates, hour_aggregates, hour)
        spy_aggs = call_polygon_spy(from_stamp, to_stamp, timespan="day", multiplier="1")
        current_spy = call_polygon_spyH(hour_stamp, hour_stamp, timespan="hour", multiplier="1", hour=hour)
        current_spy = current_spy.values[0]
        # spy_aggs = full_aggs.loc[full_aggs['symbol']=="SPY"]
        df = build_analytics(full_aggs, hour)
        df.reset_index(drop=True, inplace=True)
        df = df.groupby("symbol").tail(1)
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
        # alerts_dict = build_alerts(df)
        # for key, df in alerts_dict.items():
        csv = df.to_csv()
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"fixed_alerts_full/new_features/big_fish_stable/{key_str}/{hour}.csv", Body=csv)
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
    end_day = end - timedelta(days=1)
    to_stamp = end_day.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp

# def build_price_action(df):
#     alerts = df.groupby("symbol").tail(1)
#     alerts.reset_index(drop=True, inplace=True)
#     result = alerts.apply(calc_price_action, axis=1)
#     result.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
#     result.reset_index()
#     price = pd.DataFrame(result.to_list())
#     alerts['one_max'] = price['one_max']
#     alerts['one_min'] = price['one_min']
#     alerts['one_pct'] = price['one_pct']
#     alerts['three_max'] = price['three_max']
#     alerts['three_min'] = price['three_min']
#     alerts['three_pct'] = price['three_pct']
#     return alerts

def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    for index, value in enumerate(aggregates):
        hour_aggs = hour_aggregates[index]
        hour_aggs = hour_aggs.loc[hour_aggs["hour"] > 9]
        hour_aggs = hour_aggs.loc[hour_aggs["hour"] <= int(hour)]
        volume = hour_aggs.v.sum()
        open = hour_aggs.o.iloc[0]
        close = hour_aggs.c.iloc[-1]
        high = hour_aggs.h.max()
        low = hour_aggs.l.min()
        n = hour_aggs.n.sum()
        t = hour_aggs.t.iloc[-1]
        hour_dict = {"v": volume, "vw":0, "o":open, "c":close, "h":high, "l":low, "t":t,"n":n,"date":hour_aggs.date.iloc[-1],"hour":hour,"symbol":hour_aggs.symbol.iloc[-1]}
        aggs_list = [volume, 0, open, close, high, low, t, n, hour_aggs.date.iloc[-1], hour, hour_aggs.symbol.iloc[-1]]
        value.loc[len(value)] = aggs_list
        full_aggs.append(value)
    return full_aggs


def build_alerts(alerts):
    # alerts = df.groupby("symbol").tail(1)
    # alerts.reset_index(drop=True, inplace=True)
    # price = alerts.apply(calc_price_action, axis=1)
    # price = pd.DataFrame(price.to_list())
    # price.columns = ['one_max', 'one_min', 'one_pct', 'three_max', 'three_min', 'three_pct']
    # alerts = alerts.join([price])
    # alerts['one_max'] = price['one_max']
    # alerts['one_min'] = price['one_min']
    # alerts['one_pct'] = price['one_pct']
    # alerts['three_max'] = price['three_max']
    # alerts['three_min'] = price['three_min']
    # alerts['three_pct'] = price['three_pct']
    c_sorted = alerts.sort_values(by="close_diff", ascending=False)
    v_sorted = alerts.sort_values(by="v", ascending=False)
    vdiff_sorted = alerts.sort_values(by="v_diff_pct", ascending=False)
    gainers = c_sorted.head(50)
    losers = c_sorted.tail(50)
    # gt = c_sorted.loc[c_sorted["close_diff"] > 0.025]
    # lt = c_sorted.loc[c_sorted["close_diff"] < -0.025]
    most_active = v_sorted.head(50)
    volume_gain = vdiff_sorted.head(50)
    return {"all_alerts":alerts, "gainers": gainers, "losers": losers, "most_actives":most_active, "vdiff_gain":volume_gain}

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
    start_date = datetime(2018,10,1)
    end_date = datetime(2020,1,1)
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
    #     build_historic_data("2021-01-04")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(build_historic_data, date_str) for date_str in date_list]
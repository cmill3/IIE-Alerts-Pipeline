import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import call_polygon_hist, build_analytics, get_pcr_historic, calc_price_action, calc_vdiff
from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")

weekly_expiries = ['SPY', 'IVV', 'QQQ', 'GLD', 'IWM', 'EFA', 'XLK', 'XLV', 'TLT', 'LQD', 'XLE', 'TQQQ', 'SQQQ', 'SPXS', 'SPXL', 'SOXL', 'SOXS', 'MMM', 'ABT', 'ABBV', 'ACN', 'ATVI', 'ADM', 'ADBE', 'ADP', 
                   'AAP', 'AFL', 'ALB', 'ALGN', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMD', 'AAL', 'AXP', 'AIG', 'ABC', 'AMGN', 'ADI', 'APA', 'AAPL', 'AMAT', 'ANET', 'T', 'ADSK', 'BAC', 'BBWI', 'BAX', 'BBY', 'BIIB', 
                   'BLK', 'BA', 'BKNG', 'BMY', 'AVGO', 'CZR', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CAT', 'CBOE', 'CNC', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CI', 'CSCO', 'C', 'CLX', 'CME', 'KO', 'CMCSA', 'CMA', 'CAG', 
                   'COP', 'STZ', 'GLW', 'COST', 'CTRA', 'CSX', 'CVS', 'DHI', 'DHR', 'DE', 'DAL', 'DVN', 'DLR', 'DFS', 'DISH', 'DIS', 'DG', 'DLTR', 'DPZ', 'DOW', 'DD', 'EBAY', 'EA', 'ELV', 'LLY', 'EMR', 'ENPH', 'EOG', 'EQT', 
                   'ETSY', 'EXPE', 'XOM', 'FDX', 'FITB', 'FSLR', 'FI', 'F', 'FTNT', 'FOXA', 'FCX', 'GEHC', 'GNRC', 'GD', 'GE', 'GM', 'GILD', 'GS', 'HAL', 'HSY', 'HES', 'HD', 'HON', 'HRL', 'HPQ', 'HUM', 'HBAN', 'IBM', 'ILMN', 
                   'INTC', 'IP', 'INTU', 'ISRG', 'JNJ', 'JPM', 'JNPR', 'KEY', 'KMB', 'KMI', 'KLAC', 'KHC', 'KR', 'LRCX', 'LVS', 'LEN', 'LMT', 'LOW', 'MRO', 'MPC', 'MAR', 'MA', 'MTCH', 'MCD', 'MCK', 'MDT', 'MRK', 'META', 'MET', 
                   'MGM', 'MU', 'MSFT', 'MRNA', 'MDLZ', 'MS', 'MOS', 'NTAP', 'NFLX', 'NEM', 'NKE', 'NSC', 'NOC', 'NCLH', 'NUE', 'NVDA', 'NXPI', 'OXY', 'ON', 'ORCL', 'PARA', 'PYPL', 'PEP', 'PFE', 'PCG', 'PM', 'PSX', 'PXD', 'PNC', 
                   'PPG', 'PG', 'PHM', 'QCOM', 'RTX', 'REGN', 'ROST', 'RCL', 'SPGI', 'CRM', 'SLB', 'STX', 'NOW', 'SWKS', 'SEDG', 'SO', 'LUV', 'SBUX', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TSLA', 'TXN', 'TMO', 'TJX', 'TSCO', 'TFC', 
                   'TSN', 'USB', 'ULTA', 'UNP', 'UAL', 'UPS', 'URI', 'UNH', 'VLO', 'VZ', 'VRTX', 'VFC', 'V', 'WBA', 'WMT', 'WBD', 'WM', 'WFC', 'WDC', 'WHR', 'WMB', 'WYNN', 'ZION']
# hours = [10,11,12,13,14,15]
now_str = datetime.now().strftime("%Y/%m/%d/%H:%M")
s3 = boto3.client('s3')
logger = logging.getLogger()
def alerts_consolidation(event, context):
    date = datetime.now()
    key_str = date.strftime("%Y/%m/%d")
    distibuted_numbers = [1,2,3]
    hour = (date.hour - 4)
    s3 = get_s3_client()
    dfs = []
    for distibuted_number in distibuted_numbers:
        obj = s3.get_object(Bucket="inv-alerts", Key=f"distributed_alerts/{key_str}/{hour}_{distibuted_number}.csv")
        df = pd.read_csv(obj['Body'])
        dfs.append(df)
    full_df = pd.concat(dfs)
    alerts_dict = build_alerts(full_df)
    for key, df in alerts_dict.items():
        try:
            csv = df.to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"{key}/{key_str}/{hour}.csv", Body=csv)
            # put_response = s3.put_object(Bucket="icarus-research-data", Key=f"inv_alerts_with_price_expanded/{key}/{key_str}/{hour}.csv", Body=csv)
        except ClientError as e:
            logging.error(f"error for {key} :{e})")
            continue
    return put_response

def build_alerts(df):
    alerts = alerts.loc[alerts["symbol"].isin(weekly_expiries)]
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
    return {"all_alerts":alerts, "gainers": gainers, "losers": losers, "gt":gt, "lt":lt, "most_actives":most_active, "vdiff_gain":volume_gain, "vdiff_loss":volume_loss}

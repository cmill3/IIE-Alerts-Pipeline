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
import warnings
warnings.filterwarnings("ignore")

alerts_bucket = os.getenv("ALERTS_BUCKET")
all_sym = ['SPY', 'IVV', 'VOO', 'VTI', 'QQQ', 'VEA', 'IEFA', 'VTV', 'BND', 'AGG', 'VUG', 'VWO', 'IEMG', 'IWF', 'VIG', 
'IJH', 'IJR', 'GLD', 'VGT', 'VXUS', 'VO', 'IWM', 'BNDX', 'EFA', 'IWD', 'VYM', 'SCHD', 'XLK', 'ITOT', 'VB', 'VCIT', 
'XLV', 'TLT', 'BSV', 'VCSH', 'LQD', 'XLE', 'VEU', 'RSP', 'TQQQ', 'SQQQ', 'SPXS', 'SPXL', 'SOXL', 'SOXS', 'MMM', 'AOS', 
'ABT', 'ABBV', 'ACN', 'ATVI', 'ADM', 'ADBE', 'ADP', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALGN', 
'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AMD', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 
'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'AON', 'APA', 'AAPL', 'AMAT', 'APTV', 'ACGL', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 
'ADSK', 'AZO', 'AVB', 'AVY', 'AXON', 'BKR', 'BAC', 'BBWI', 'BAX', 'BDX', 'WRB', 'BRK.B', 'BBY', 'BIO', 'TECH', 'BIIB', 'BLK', 
'BK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'BRO', 'BF.B', 'BG', 'CHRW', 'CDNS', 'CZR', 'CPT', 'CPB', 'COF', 
'CAH', 'KMX', 'CCL', 'CARR', 'CTLT', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CDAY', 'CF', 'CRL', 'SCHW', 'CHTR', 'CVX', 
'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'COP', 
'ED', 'STZ', 'CEG', 'COO', 'CPRT', 'GLW', 'CTVA', 'CSGP', 'COST', 'CTRA', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 
'DAL', 'XRAY', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DISH', 'DIS', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK', 'DD', 'DXC', 'EMN', 
'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'LLY', 'EMR', 'ENPH', 'ETR', 'EOG', 'EPAM', 'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 
'ETSY', 'RE', 'EVRG', 'ES', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FDS', 'FICO', 'FAST', 'FRT', 'FDX', 
'FITB', 'FSLR', 'FE', 'FIS', 'FLT', 'FMC', 'F', 'FTNT', 'FTV', 'FOXA', 'FOX', 'BEN', 'FCX', 'GRMN', 'IT', 'GNRC', 
'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'HAL', 'HIG', 'HAS', 'HCA', 'PEAK', 'HSIC', 'HSY', 'HES', 
'HPE', 'HLT', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUM', 'HBAN', 'HII', 'IBM', 'IEX', 'IDXX', 'ITW', 
'ILMN', 'INCY', 'IR', 'PODD', 'INTC', 'ICE', 'IFF', 'IP', 'IPG', 'INTU', 'ISRG', 'IVZ', 'INVH', 'IQV', 'IRM', 'JBHT', 
'JKHY', 'J', 'JNJ', 'JCI', 'JPM', 'JNPR', 'K', 'KDP', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KHC', 'KR', 'LHX', 
'LH', 'LRCX', 'LW', 'LVS', 'LDOS', 'LEN', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LYB', 'MTB', 'MRO', 'MPC', 
'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MTCH', 'MKC', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 
'MSFT', 'MAA', 'MRNA', 'MHK', 'MOH', 'TAP', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'NDAQ', 'NTAP', 
'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NKE', 'NI', 'NDSN', 'NSC', 'NTRS', 'NOC', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 
'NXPI', 'ORLY', 'OXY', 'ODFL', 'OMC', 'ON', 'OKE', 'ORCL', 'OGN', 'OTIS', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 
'PEP', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'POOL', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PTC', 
'PSA', 'PHM', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 
'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SJM', 'SNA', 
'SEDG', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STLD', 'STE', 'SYK', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 
'TPR', 'TRGP', 'TGT', 'TEL', 'TDY', 'TFX', 'TER', 'TSLA', 'TXN', 'TXT', 'TMO', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 
'TRMB', 'TFC', 'TYL', 'TSN', 'USB', 'UDR', 'ULTA', 'UNP', 'UAL', 'UPS', 'URI', 'UNH', 'UHS', 'VLO', 'VTR', 'VRSN', 
'VRSK', 'VZ', 'VRTX', 'VFC', 'VTRS', 'VICI', 'V', 'VMC', 'WAB', 'WBA', 'WMT', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 
'WST', 'WDC', 'WRK', 'WY', 'WHR', 'WMB', 'WTW', 'GWW', 'WYNN', 'XEL', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS', 
'CVNA', 'DKNG', 'VXX', 'W', 'CHWY', 'PTON', 'TEAM', 'MDB', 'HOOD', 'MARA', 'AI', 'LYFT', 'BYND', 'RIOT', 'U', 'DOCU', 'TTD',
'UBER', 'JD', 'DDOG', 'CRWD', 'SQ', 'RBLX', 'SNAP', 'FUTU', 'TSM', 'LCID', 'UPST', 'TDOC', 'SNOW', 'BIDU', 'NIO', 'SHOP', 'ROKU', 
'OKTA', 'RIVN', 'ZM', 'WBD', 'SE', 'SOFI', 'META', 'TWLO', 'ZS', 'BABA', 'PLTR', 'PINS', 'PANW', 'ABNB', 'NET', 'COIN', 'BILI', 'ARM']
all_symbols = ['ZM', 'UBER', 'TDOC', 'UAL', 'RCL', 'AMZN', 'ABNB', 'TSLA', 'SQ', 'SHOP', 'DOCU', 'TWLO', 'DDOG', 'ZS', 
'OKTA', 'ETSY', 'PINS', 'FUTU', 'BIDU', 'JD', 'BABA', 'AMD', 'NVDA', 'PYPL', 'PLTR', 'NFLX', 'CRWD', 'MRNA', 'SNOW', 
'SOFI', 'CHWY', 'TTD', 'NOW', 'TEAM', 'MDB', 'HOOD', 'LYFT', 'AAL', 'CZR', 'ARM', 'NCLH', 'MU', 'WBD', 'CCL', 'AMAT', 
'SNAP',"META","FB",'CMG', 'AXP', 'DAL', 'MMM', 'PEP', 'GE', 'MRK', 'HD', 'LOW', 'VZ', 'PG', 'TSM', 'GOOG', 'GOOGL', 'BAC', 
'AAPL', 'CRM', 'MSFT', 'F', 'V', 'MA', 'JNJ', 'DIS', 'JPM', 'ADBE', 'BA', 'CVX', 'PFE', 'C', 'CAT', 'KO', 'MS', 'GS', 'IBM', 
'CSCO', 'WMT', 'WFC', 'TGT', 'COST', 'INTC', 'PANW', 'ORCL', 'SBUX', 'NKE', 'XOM', 'RTX', 'UPS', 'FDX', 'LMT', 'GIS', 'KHC', 'AVGO', 
'QCOM', 'TXN', 'MGM','GM']




def run_process(date_str):
    try:
        print(date_str)
        alerts_runner(date_str)
    except Exception as e:
        print(f"{date_str} {e}")
        alerts_runner(date_str)
    print(f"Finished {date_str}")

def fix_alerts(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        all_symbol = s3.get_object(Bucket="inv-alerts", Key=f"all_alerts/vol_features/{key_str}/{hour}.csv")
        all_symbol_df = pd.read_csv(all_symbol['Body'])
        all_symbol_df.drop(columns=['Unnamed: 0','Unnamed: 0.1'], inplace=True)
        gm = all_symbol_df.loc[all_symbol_df['symbol'] == 'GM']
        new_columns = [col.replace('_x', '') for col in all_symbol_df.columns]
        all_symbol_df.columns = new_columns
        y_columns = all_symbol_df.filter(regex='_y').columns
        all_symbol_df.drop(columns=y_columns, inplace=True)
        # new_gm = [col.replace('_y', '') for col in gm.columns]
        # gm.columns = new_gm
        # print(gm)
        # print(all_symbol_df.head(3))
        # df = pd.merge(all_symbol_df, gm, on=['symbol','hour','date'])
        # df = df.drop_duplicates(subset=['symbol'])
        put_response = s3.put_object(Bucket="inv-alerts", Key=f"all_alerts/vol_fix/{key_str}/{hour}.csv", Body=all_symbol_df.to_csv())
    
def alerts_runner(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        all_symbol = s3.get_object(Bucket="inv-alerts", Key=f"all_alerts/vol_fix/{key_str}/{hour}.csv")
        all_symbol_df = pd.read_csv(all_symbol['Body'])
        all_symbol_df = all_symbol_df.loc[all_symbol_df['symbol'].isin(all_symbols)]
        alerts = build_alerts(all_symbol_df)
        for alert in alerts:
            csv = alerts[alert].to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/{key_str}/{alert}/{hour}.csv", Body=csv)

def add_data_to_alerts(date_str):
    hours = ["10","11","12","13","14","15"]
    key_str = date_str.replace("-","/")
    s3 = boto3.client('s3')
    from_stamp, to_stamp, hour_stamp = generate_dates_historic(date_str)
    for hour in hours:
        for type in ['gainers','losers']:
            alert = s3.get_object(Bucket="inv-alerts", Key=f"bf_alerts/{key_str}/{type}/{hour}.csv")
            sf = s3.get_object(Bucket="inv-alerts", Key=f"sf/vol/{key_str}/{hour}.csv")
            sf= pd.read_csv(sf['Body'])
            bf = s3.get_object(Bucket="inv-alerts", Key=f"bf/vol/{key_str}/{hour}.csv")
            bf= pd.read_csv(bf['Body'])
            alert = pd.read_csv(alert['Body'])
            full_data = pd.concat([sf,bf])
            full_df = full_data.loc[full_data['symbol'].isin(alert['symbol'])]
            alert = alert[['symbol','hour']]
            alerts_data = alert.merge(full_df, on=['symbol','hour'])
            alerts_data = alerts_data.drop_duplicates(subset=['symbol'])
            s3.put_object(Bucket="inv-alerts", Key=f"bf_alerts/data/{key_str}/{type}/{hour}.csv", Body=alerts_data.to_csv())
    print(f"Finished with {date_str}")
        

def combine_hour_aggs(aggregates, hour_aggregates, hour):
    full_aggs = []
    for index, value in enumerate(aggregates):
        hour_aggs = hour_aggregates[index]
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
        value['close_diff'] = value['c'].pct_change().round(4)
        full_aggs.append(value)
    return full_aggs

def build_alerts(df):
    df['cd_vol'] = df['close_diff']/df['oneD_stddev50'].round(3)
    df['cd_vol3'] = df['close_diff3']/df['threeD_stddev50'].round(3)
    volume_sorted = df.sort_values(by="v", ascending=False)
    v_sorted = df.sort_values(by="hour_volume_vol_diff_pct", ascending=False)
    c_sorted = df.sort_values(by="close_diff", ascending=False)
    cvol_sorted = df.sort_values(by="cd_vol", ascending=False)
    cvol3_sorted = df.sort_values(by="cd_vol3", ascending=False)
    gainers = c_sorted.head(30)
    losers = c_sorted.tail(30)
    v_diff = v_sorted.head(30)
    cdvol_gainers = cvol_sorted.head(30)
    cdvol_losers = cvol_sorted.tail(30)
    cdvol3_gainers = cvol3_sorted.head(30)
    cdvol3_losers = cvol3_sorted.tail(30)
    volume = volume_sorted.head(30)
    return {"gainers": gainers, "losers": losers, "v_diff": v_diff, "most_actives": volume,"cdvol_gainers": cdvol_gainers, "cdvol_losers": cdvol_losers, "cdvol3_gainers": cdvol3_gainers, "cdvol3_losers": cdvol3_losers}

def generate_dates_historic(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=8)
    # end_day = end - timedelta(days=1)
    to_stamp = end.strftime("%Y-%m-%d")
    hour_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp, hour_stamp


if __name__ == "__main__":
    # build_historic_data(None, None)
    print(os.cpu_count())
    start_date = datetime(2018,1,1)
    end_date = datetime(2023,12,23)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)

    # # for date_str in date_list:
    # add_data_to_alerts("2022-01-27")
        

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_process, date_str) for date_str in date_list]
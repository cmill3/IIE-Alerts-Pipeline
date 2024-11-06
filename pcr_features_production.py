import pandas as pd
import boto3
import logging
from helpers.data import execute_polygon_call
from datetime import datetime, timedelta
import pytz
import json

est = pytz.timezone('US/Eastern')
date = datetime.now(est)
s3 = boto3.client('s3')
logger = logging.getLogger()

def run_process(event,context):
    build_pcr_features()
    logger.info(f"Finished {date}")

def generate_dates(date_str):
    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(weeks=4)
    to_stamp = end.strftime("%Y-%m-%d")
    from_stamp = start.strftime("%Y-%m-%d")
    return from_stamp, to_stamp

def build_pcr_features():
    year,month,day,hour = date.year, date.month, date.day, date.hour
    dt = datetime(year,month,day)
    key_str = dt.strftime("%Y/%m/%d")
    symbols = ["SPY"]
    options_snapshots = []
    query_data = build_query_params(symbols, dt)
    ## for now just do SPY
    data = query_data[0]
    options_snapshot = polygon_call_pcr(data)
    s3.put_object(Bucket="icarus-research-data", Key=f"options_snapshot/{key_str}/{hour}/SPY.csv", Body=options_snapshot.to_csv())
    hist_options_snapshot = options_snapshot

def pull_hist_options_snapshot(date_str):
    pass

def build_query_params(symbols,dt):
    full_query_data = []
    for symbol in symbols:
        query_data = {}
        if symbol in ["SPY","QQQ","IWM"]:
            query_data['symbol'] = symbol
            query_data['expiration_dates'] = build_expiration_dates(dt,index=True)
        else:
            query_data['symbol'] = symbol
            query_data['expiration_dates'] = build_expiration_dates(dt,index=False)
        full_query_data.append(query_data)
    return full_query_data
    
def build_expiration_dates(date,index):
    expiration_dates = []
    if index:
        for i in range(0, 21):
            expiry_date = date + timedelta(days=i)
            if expiry_date.weekday() < 5:
                expiration_dates.append((date + timedelta(days=i)).strftime("%Y-%m-%d"))
    else:
        now_weekday = date.weekday()
        fri_diff = 4 - now_weekday
        if fri_diff == 0:
            expiration_dates = [date.strftime("%Y-%m-%d"),(date + timedelta(days=7)).strftime("%Y-%m-%d"), (date + timedelta(days=14)).strftime("%Y-%m-%d"), (date + timedelta(days=21)).strftime("%Y-%m-%d")]
        else:
            friday = now + timedelta(days=fri_diff)
            expiration_dates = [friday.strftime("%Y-%m-%d"), (friday + timedelta(days=7)).strftime("%Y-%m-%d"), (friday + timedelta(days=14)).strftime("%Y-%m-%d")]

    return expiration_dates

def polygon_call_pcr(query_data):
    dicts_for_df = []
    for contract_type in ["call","put"]:
        for expiration_date in query_data['expiration_dates']:
            url = f"https://api.polygon.io/v3/snapshot/options/{query_data['symbol']}?expiration_date={expiration_date}&contract_type={contract_type}&limit=250&apiKey={KEY}"
            response = execute_polygon_call(url)
            if response.status_code == 200:
                response_data = json.loads(response.text)
                try:
                    results = response_data['results']
                    for result in results:
                        try:
                            pcr_dict = result["day"]
                            pcr_dict['underlying_symbol'] = query_data['symbol']
                            pcr_dict['option_symbol'] = result['details']['ticker']
                            pcr_dict['contract_type'] = contract_type
                            pcr_dict['expiration_date'] = expiration_date
                            pcr_dict['strike_price'] = result['details']['strike_price']
                            dicts_for_df.append(pcr_dict)
                        except Exception as e:
                            print(f"Error with {url} {e}")
                            continue
                except Exception as e:
                    logging.error(f"No results {url} {e}")
                    continue
            else:
                logging.error(f"Error with {url} {response.status_code} {response.text}")
    full_df = pd.DataFrame.from_dict(dicts_for_df)
    return full_df

def create_pcr_features(data):
    ## creates the pcr features
    pass

if __name__ == "__main__":
    run_process(None,None)

# "results":[
#     {
#         "break_even_price":524.95,
#         "day":{"change":-1.03,"change_percent":-25.8,"close":2.96,"high":3.42,"last_updated":1717041600000000000,"low":2.38,"open":3.12,"previous_close":3.99,"volume":3706,"vwap":2.8363},
#         "details":{"contract_type":"call","exercise_style":"american","expiration_date":"2024-06-04","shares_per_contract":100,"strike_price":451,"ticker":"O:SPY240604C00451000"},
#         "greeks":{"delta":0.9892951434244974,"gamma":0.0007824807925531281,"theta":-0.13994766379039422,"vega":0.012713581376400981},
#         "implied_volatility":0.5624305671328997,
#         "last_quote":{"ask":74.16,"ask_size":75,"ask_exchange":302,"bid":73.74,"bid_size":75,"bid_exchange":302,"last_updated":1717077435713253120,"midpoint":73.95,"timeframe":"REAL-TIME"},
#         "last_trade":{},
#         "open_interest":0,"underlying_asset":{"change_to_break_even":0.39,"last_updated":1717077437082900736,"price":524.56,"ticker":"SPY","timeframe":"REAL-TIME"}
#         },
#     {"break_even_price":524.95,"day":{},"details":{"contract_type":"call","exercise_style":"american","expiration_date":"2024-06-04","shares_per_contract":100,"strike_price":452,"ticker":"O:SPY240604C00452000"},"greeks":{"delta":0.9892190761936802,"gamma":0.0007982271071980272,"theta":-0.1394775838875239,"vega":0.012724140133022524},"implied_volatility":0.5550287269185773,"last_quote":{"ask":73.16,"ask_size":75,"ask_exchange":302,"bid":72.74,"bid_size":75,"bid_exchange":302,"last_updated":1717077435712917248,"midpoint":72.95,"timeframe":"REAL-TIME"},"last_trade":{},"open_interest":0,"underlying_asset":{"change_to_break_even":0.39,"last_updated":1717077437082900736,"price":524.56,"ticker":"SPY","timeframe":"REAL-TIME"}}
import requests
import json
import pandas as pd
from datetime import datetime
import pandas_ta as ta

key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"

def format_pcr_dates(dates):
    date_str = dates.apply(lambda x: x.strftime("%Y-%m-%d"))
    # cut_date = date_str.apply(lambda x: x.split(" ")[0])
    formatted_dates = date_str.apply(lambda x: datetime.strptime(x, '%Y-%m-%d').strftime("%Y-%m-%dT%H:%M:%SZ"))
    return formatted_dates

def get_pcr(symbol, window, dates):
    symbol = symbol.iloc[0]
    pcr_dates = format_pcr_dates(dates)
    url = f"https://www.alphaquery.com/data/option-statistic-chart?ticker={symbol}\
        &perType={window}-Day&identifier=put-call-ratio-volume"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/70.0.3538.77 Safari/537.36"
    }
    
    r = requests.get(url, headers=headers)
    pcr = pd.DataFrame.from_dict(r.json())
    pcr.rename(columns={"x": "date", "value": "PCR"}, inplace=True)
    x = pcr.loc[pcr['date'].isin(pcr_dates)]
    raw_list = x['PCR'].values.tolist()
    raw_list.insert(0,0)
    print(raw_list)
    return raw_list

def get_pcr_historic(symbol, window, dates):
    symbol = symbol.iloc[0]
    pcr_dates = format_pcr_dates(dates)
    url = f"https://www.alphaquery.com/data/option-statistic-chart?ticker={symbol}\
        &perType={window}-Day&identifier=put-call-ratio-volume"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/70.0.3538.77 Safari/537.36"
    }
    
    r = requests.get(url, headers=headers)
    pcr = pd.DataFrame.from_dict(r.json())
    pcr.rename(columns={"x": "date", "value": "PCR"}, inplace=True)
    x = pcr.loc[pcr['date'].isin(pcr_dates)]
    raw_list = x['PCR'].values.tolist()
    return raw_list

def call_polygon(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    error_list = []

    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"

        response = requests.request("GET", url, headers=headers, data=payload)

        response_data = json.loads(response.text)
        try:
            results = response_data['results']
        except:
            error_list.append(symbol)
            continue
        results_df = pd.DataFrame(results)
        results_df['symbol'] = symbol
        dfs.append(results_df)

    return dfs, error_list

def build_analytics(aggregates, pcr_func):
    indicators = []

    for d in aggregates:

        d['t'] = d['t'].apply(lambda x: int(x/1000))
        d['date'] = d['t'].apply(lambda x: datetime.fromtimestamp(x))
        d['time'] = d['date'].apply(lambda x: str(x).split(" ")[1])
        d['day'] = d.date.dt.day


        try: 
            d['price7'] = ta.slope(d['c'],7)    
            d['price14'] = ta.slope(d['c'],14) 
            d['vol7'] = ta.slope(d['v'],7)    
            d['vol14'] = ta.slope(d['v'],14)
            d['volume_10MA'] = d['v'].rolling(10).mean()
            d['volume_25MA'] = d['v'].rolling(25).mean()
            d['volume_10DDiff'] = d.apply(lambda x: x.v - x.volume_10MA, axis=1)
            d['volume_25DDiff'] = d.apply(lambda x: x.v - x.volume_25MA, axis=1)
            d['rsi'] = ta.rsi(d['c'])
            d['roc'] = ta.roc(d['c'])
            d['roc3'] = ta.roc(d['c'],length=3)
            d['roc5'] = ta.roc(d['c'],length=5)
            d['cmf'] = ta.cmf(d['h'], d['l'], d['c'], d['v'])
            d['close_diff'] = d['c'] - d['c'].shift(1)
            d['volume_diff'] = (d['v'] - d['v'].shift(1)/d['v'].shift(1))*100
            try:
                pcr_list = pcr_func(d['symbol'],10,d['date'])
                d['PCR'] = pcr_list
            except Exception as e:
                print(f"Error: {e}")
                d['PCR'] = 111
            adx = ta.adx(d['h'],d['l'],d['c'])
            d['adx'] = adx['ADX_14']
            macd = ta.macd(d['c'])
            d['macd'] = macd.MACD_12_26_9
            indicators.append(d)
        except Exception as e:
            print(f"Error: {e}")
            print(d)
    
    df = pd.concat(aggregates).round(3)

    return df
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
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

def calc_price_action(row):
    # from_stamp = row['date'].split(" ")[0]
    to_date = row['date'] + timedelta(days=7)
    to_stamp = to_date.strftime("%Y-%m-%d")
    from_stamp = row['date'].strftime("%Y-%m-%d")
    aggs = call_polygon_price([row['symbol']], from_stamp, to_stamp, "hour", 1)
    one_day, three_day = build_date_dfs(aggs, row['t'])
    open = one_day.head(1)['o'].values[0]
    one_c = one_day.tail(1)['c'].values[0]
    one_h = one_day['h'].max()
    one_l = one_day['l'].min()
    three_c = three_day.tail(1)['c'].values[0]
    three_h = three_day['h'].max()
    three_l = three_day['l'].min()
    one_high = (one_h - open)/ open
    one_low = (one_l - open)/ open
    one_pct = (one_c - row['c'])/row['c']
    three_high = (three_h - open)/ open
    three_low = (three_l - open)/ open
    three_pct = (three_c - row['c'])/row['c']
    return {"one_max": one_high, "one_min": one_low, "one_pct": one_pct, "three_max": three_high, "three_min": three_low, "three_pct": three_pct}

def build_date_dfs(df, t):
    dt = df["date"].iloc[0]
    d1, d2, d3 = determine_num_days(dt)
    one_day = (dt + timedelta(days=d1)).day
    two_day = (dt + timedelta(days=d2)).day
    three_day = (dt + timedelta(days=d3)).day
    one_day_list = [dt.day, one_day]
    three_day_list = [dt.day, one_day, two_day, three_day]
    future = df.loc[df['t'] > t]
    one_day_df = future.loc[future['date'].dt.day.isin(one_day_list)]
    three_day_df = future.loc[future['date'].dt.day.isin(three_day_list)]
    return one_day_df, three_day_df

def determine_num_days(dt):
    day_of_week = dt.weekday()
    if day_of_week < 2:
        return 1,2,3
    if day_of_week == 2:
        return 1,2,5
    if day_of_week == 3:
        return 1,4,5
    if day_of_week == 4:
        return 3,4,5

def call_polygon(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    error_list = []

    if timespan == "minute":
        from_stamp = to_stamp
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
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x: datetime.fromtimestamp(x))
        results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        results_df['symbol'] = symbol
        dfs.append(results_df)

    return dfs, error_list

def call_polygon_hist(symbol_list, from_stamp, to_stamp, timespan, multiplier, hour):
    payload={}
    headers = {}
    dfs = []
    
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    error_list = []

    if timespan == "minute":
        from_stamp = to_stamp
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
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x: datetime.fromtimestamp(x))
        results_df['hour'] = hour
        results_df['symbol'] = symbol
        dfs.append(results_df)

    return dfs, error_list

def call_polygon_price(symbol, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    error_list = []
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol[0]}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"

    response = requests.request("GET", url, headers=headers, data=payload)

    response_data = json.loads(response.text)
    results = response_data['results']
    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['day'] = results_df['date'].apply(lambda x: x.day)
    # results_df['symbol'] = row['symbol']
    # dfs.append(results_df)

    return results_df

def calc_vdiff(row):
    try:
        to_stamp = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
        from_stamp = to_stamp - timedelta(days=10)
        from_str = from_stamp.strftime("%Y-%m-%d")
        to_str = row['date'].split(" ")[0]
        aggs = call_polygon_hist([row['symbol']], from_str, to_str, "day", 1)
        v = aggs.iloc[-1]['v']
        v_1 = aggs.iloc[-2]['v']
        v_1_avg = (v_1/7)
        v_avg = (v/(abs(9-row['hour'])+1))
        v_diff_pct = (v_avg - v_1_avg) / v_1_avg
        return v_diff_pct
    except Exception as e:
        print(e)
        print(row['symbol'])
        return 0.111021999
    
def calc_vdiff_pipeline(volumes, hour):
    metrics = []
    for index ,v in enumerate(volumes):
        if index == 0 or index == 1:
            metrics.append(0.111021999)
            continue
        else:
            v_1 = volumes[index-1]
            v_1_avg = (v_1/7)
            v_avg = (v/(abs(9-hour)+1))
            v_diff_pct = (v_avg - v_1_avg) / v_1_avg
            metrics.append(v_diff_pct)
    return metrics
   

def build_analytics(aggregates, pcr_func, hour):
    indicators = []
    for d in aggregates:
    #     try:
    #         d = data.copy()
    #         symbol = d['symbol'].iloc[0]
    #         hour_aggs = hour_aggregates.loc[hour_aggregates['symbol'] == symbol]
    #         hour_aggs = hour_aggs.loc[hour_aggs['hour'] == hour]
    #         values = hour_aggs.to_dict('records')[0]
    #         d.loc[len(d.index)] = values
    #         # d['t'] = d['t'].apply(lambda x: int(x/1000))
    #         # d['date'] = d['t'].apply(lambda x: datetime.fromtimestamp(x))
    #         d['time'] = d['date'].apply(lambda x: str(x).split(" ")[1])
    #         # d['day'] = d.date.dt.day
    #     except Exception as e:
    #         print(e)
    #         continue

        try: 
            d['price7'] = ta.slope(d['c'],7)    
            d['price14'] = ta.slope(d['c'],14) 
            d['vol7'] = ta.slope(d['v'],7)    
            d['vol14'] = ta.slope(d['v'],14)
            d['volume_10MA'] = d['v'].rolling(10).mean()
            d['volume_25MA'] = d['v'].rolling(25).mean()
            d['price_10MA'] = d['c'].rolling(10).mean()
            d['price_25MA'] = d['c'].rolling(25).mean()
            d['volume_10DDiff'] = d.apply(lambda x: ((x.v - x.volume_10MA)/x.volume_10MA)*100, axis=1)
            d['volume_25DDiff'] = d.apply(lambda x: ((x.v - x.volume_25MA)/x.volume_25MA)*100, axis=1)
            d['price_10DDiff'] = d.apply(lambda x: ((x.c - x.price_10MA)/x.price_10MA)*100, axis=1)
            d['price_25DDiff'] = d.apply(lambda x: ((x.c - x.price_25MA)/x.price_25MA)*100, axis=1)
            d['rsi'] = ta.rsi(d['c'])
            d['roc'] = ta.roc(d['c'])
            d['roc3'] = ta.roc(d['c'],length=3)
            d['roc5'] = ta.roc(d['c'],length=5)
            d['cmf'] = ta.cmf(d['h'], d['l'], d['c'], d['v'])
            d['close_diff'] = ((d['c'] - d['c'].shift(1))/d['c'].shift(1))*100
            d['v_diff_pct'] = calc_vdiff_pipeline(d['v'].tolist(), hour)
            try:
                pcr_list = pcr_func(d['symbol'],10,d['date'])
                d['PCR'] = pcr_list
            except Exception as e:
                print(f"Error: {e}, PCR")
                d['PCR'] = 111
            adx = ta.adx(d['h'],d['l'],d['c'])
            d['adx'] = adx['ADX_14']
            # macd = ta.macd(d['c'])
            # d['macd'] = macd.MACD_12_26_9
            indicators.append(d)
        except Exception as e:
            print(f"Error: {e}")
            continue
        
        
    
    df = pd.concat(indicators).round(3)

    return df


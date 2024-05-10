def call_polygon_histD(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    trading_hours = [9,10,11,12,13,14,15]
    
    error_list = []

    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

        response = execute_polygon_call(url)

        try:
            response_data = json.loads(response.text)
            results = response_data['results']
            results_df = pd.DataFrame(results)
            results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
            results_df['date'] = results_df['t'].apply(lambda x:convert_timestamp_est(x))
            results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
            results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
            results_df['symbol'] = symbol
            results_df['day_of_week'] = results_df['date'].apply(lambda x: x.weekday())
            trimmed_df = results_df.loc[results_df['hour'].isin(trading_hours)]
            filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
            filtered_df = filtered_df.loc[filtered_df['day_of_week'] < 5]
            filtered_df.set_index('date',inplace=True)
            agg_dict = {
                'v': 'sum',
                'o': 'first',
                'c': 'last',
                'h': 'max',
                'l': 'min'
            }

            # Perform resampling and aggregation
            daily_stats = filtered_df.resample('D').agg(agg_dict)
            daily_stats.dropna(inplace=True)
            daily_stats['date_stamp'] = daily_stats.index
            daily_stats['hour'] = 0
            daily_stats['symbol'] = symbol
            daily_stats['t'] = 0
            daily_stats.rename(columns={"date_stamp": "date"}, inplace=True)
            daily_stats.reset_index(drop=True, inplace=True)
            dfs.append(daily_stats)
        except Exception as e:
            error_list.append(symbol)
            continue

    return dfs, error_list

def calc_vdiff(row):
    try:
        to_stamp = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
        from_stamp = to_stamp - timedelta(days=10)
        from_str = from_stamp.strftime("%Y-%m-%d")
        to_str = row['date'].split(" ")[0]
        aggs,_ = call_polygon_hist([row['symbol']], from_str, to_str, "day", 1)
        v = aggs.iloc[-1]['v']
        v_1 = aggs.iloc[-2]['v']
        v_1_avg = (v_1/7)
        v_avg = (v/(abs(9-row['hour'])+1))
        v_diff_pct = (v_avg - v_1_avg) / v_1_avg
        return v_diff_pct
    except Exception as e:
        print(e)
        print(row['symbol'])
        print('vdiff')
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
            v_avg = (v/(abs(9-int(hour))))
            v_diff_pct = (v_avg - v_1_avg) / v_1_avg
            metrics.append(v_diff_pct)
    return metrics

def create_adjusted_volume(volumes, hour):
    metrics = []
    for index ,v in enumerate(volumes):
        if index == 0 or index == 1:
            metrics.append(0.111021999)
            continue
        else:
            v_avg = (v/(abs(9-int(hour))))
            adj_v = v_avg * 7
            metrics.append(adj_v)
    return metrics
   

def build_analytics(aggregates, hour):
    indicators = []
    for d in aggregates:
        try: 
            d['price7'] = ta.slope(d['c'],7)    
            d['price14'] = ta.slope(d['c'],14) 
            d['adjusted_volume'] = create_adjusted_volume(d['v'].tolist(), hour)
            d['vol7'] = ta.slope(d['adjusted_volume'],7)    
            d['vol14'] = ta.slope(d['adjusted_volume'],14)
            d['rsi'] = ta.rsi(d['c'],window=14)
            d['rsi3'] = ta.rsi(d['c'],window=3)
            d['rsi5'] = ta.rsi(d['c'],window=5)
            d['roc'] = ta.roc(d['c'],window=10)
            d['roc3'] = ta.roc(d['c'],window=3)
            d['roc5'] = ta.roc(d['c'],window=5)
            d['threeD_returns_close'] = d['c'].pct_change(3)
            d['oneD_returns_close'] = d['c'].pct_change(1)
            d['range_vol'] = (d['h'] - d['l'])/ d['c']
            d['range_vol5MA'] = d['range_vol'].rolling(5).mean()
            d['range_vol10MA'] = d['range_vol'].rolling(10).mean()
            d['range_vol25MA'] = d['range_vol'].rolling(25).mean()
            d['oneD_stddev50'] = statistics.stdev(d['oneD_returns_close'])
            d['threeD_stddev50'] = statistics.stdev(d['threeD_returns_close'])
            d['cmf'] = ta.cmf(d,window=20)
            try:
                d['close_diff'] = d['c'].pct_change()
            except:
                d['close_diff'] = 0
            d['close_diff3'] = d['c'].pct_change(3)
            d['close_diff5'] = d['c'].pct_change(5)
            d['v_diff_pct'] = calc_vdiff_pipeline(d['v'].tolist(), hour)
            d['adx'] = ta.adx(d,window=14)
            d['volume_10MA'] = d['adjusted_volume'].rolling(10).mean()
            d['volume_25MA'] = d['adjusted_volume'].rolling(25).mean()
            d['price_10MA'] = d['c'].rolling(10).mean()
            d['price_25MA'] = d['c'].rolling(25).mean()
            d['volume_10DDiff'] = d.apply(lambda x: ((x.adjusted_volume - x.volume_10MA)/x.volume_10MA)*100, axis=1)
            d['volume_25DDiff'] = d.apply(lambda x: ((x.adjusted_volume - x.volume_25MA)/x.volume_25MA)*100, axis=1)
            d['price_10DDiff'] = d.apply(lambda x: ((x.c - x.price_10MA)/x.price_10MA)*100, axis=1)
            d['price_25DDiff'] = d.apply(lambda x: ((x.c - x.price_25MA)/x.price_25MA)*100, axis=1)
            upper_band, lower_band, middle_band = ta.bbands(d['c'],window=20)
            d['bbu'] = upper_band
            d['bbl'] = lower_band
            d['bbm'] = middle_band
            d['macd'] = ta.macd(d['c'])
            d['bb_spread'] = (d['bbu'] - d['bbl'])/d['c']
            d['bb_trend'] = (d['c'] - d['bbm'])/d['bbm']
            d['bb_category'] = d.apply(lambda x: ta.bbands_category(x['c'],x['bbu'],x['bbl']), axis=1)
            indicators.append(d)
        except Exception as e:
            print(d.symbol)
            print('')
            print(f"In Aggs: {e}")
            indicators.append(d)
            continue
        
        
    
    df = pd.concat(indicators).round(3)

    return df

def call_polygon_spy(from_stamp, to_stamp, timespan, multiplier):   
    trading_hours = [9,10,11,12,13,14,15]
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    results = response_data['results']

    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
    results_df['symbol'] = "SPY"
    results_df = results_df.loc[results_df['hour'].isin(trading_hours)]
    results_df = results_df.loc[~((results_df['hour'] == 9) & (results_df['minute'] < 30))]
    return results_df['c'].to_list()

def call_polygon_spyH(from_stamp, to_stamp, timespan, multiplier, hour):
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    results = response_data['results']

    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['symbol'] = "SPY"
    results_df = results_df.loc[results_df['hour'] == int(hour-1)]


    return results_df['c']

def build_spy_features(df, spy_aggs, current_spy):
    SPY_diff   = (current_spy - spy_aggs.iloc[-2])/spy_aggs.iloc[-2]
    SPY_diff3  = (current_spy - spy_aggs.iloc[-4])/spy_aggs.iloc[-4]
    SPY_diff5  = (current_spy - spy_aggs.iloc[-6])/spy_aggs.iloc[-6]
    df['SPY_diff'] = (((df['close_diff']/100) - SPY_diff)/SPY_diff)
    df['SPY_diff3'] = (((df['close_diff']/100) - SPY_diff3)/SPY_diff3)
    df['SPY_diff5'] = (((df['close_diff']/100) - SPY_diff5)/SPY_diff5)
    df['SPY_1D'] = SPY_diff
    df['SPY_3D'] = SPY_diff3
    df['SPY_5D'] = SPY_diff5
    return df

def vol_feature_engineering(df, Min_aggs,Thirty_aggs):
    features = []
    agg_dict = {
            'v': 'sum',
            'o': 'first',
            'c': 'last',
            'h': 'max',
            'l': 'min'
        }
    
    for index,min_aggs in enumerate(Min_aggs):
        min_aggs.reset_index(drop=True,inplace=True)
        thirty_aggs = Thirty_aggs[index]
        thirty_aggs.set_index('date',inplace=True)

        # Perform resampling and aggregation
        hour_aggs = thirty_aggs.resample('H').agg(agg_dict)
        daily_aggs = thirty_aggs.resample('D').agg(agg_dict)

        hour_aggs.dropna(inplace=True)
        daily_aggs.dropna(inplace=True)

        min_aggs['price_change'] = abs(min_aggs['c'].pct_change())
        min_aggs['volume_change'] = abs(min_aggs['v'].pct_change())
        hour_aggs['price_change'] = abs(hour_aggs['c'].pct_change())
        hour_aggs['volume_change'] = abs(hour_aggs['v'].pct_change())
        daily_aggs['price_change'] = abs(daily_aggs['c'].pct_change())
        daily_aggs['volume_change'] = abs(daily_aggs['v'].pct_change())

        min_aggs['return_vol_15M'] = min_aggs['price_change'].rolling(window=15).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_15M'] = min_aggs['volume_change'].rolling(window=15).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_30M'] = min_aggs['price_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_30M'] = min_aggs['volume_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_60M'] = min_aggs['price_change'].rolling(window=60).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_60M'] = min_aggs['volume_change'].rolling(window=60).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_120M'] = min_aggs['price_change'].rolling(window=120).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_120M'] = min_aggs['volume_change'].rolling(window=120).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_240M'] = min_aggs['price_change'].rolling(window=240).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_240M'] = min_aggs['volume_change'].rolling(window=240).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['return_vol_450M'] = min_aggs['price_change'].rolling(window=450).apply(lambda x: abs(x).mean(), raw=True)
        min_aggs['volume_vol_450M'] = min_aggs['volume_change'].rolling(window=450).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['return_vol_4H'] = hour_aggs['price_change'].rolling(window=4).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['return_vol_8H'] = hour_aggs['price_change'].rolling(window=8).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['return_vol_16H'] = hour_aggs['price_change'].rolling(window=16).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['volume_vol_4H'] = hour_aggs['volume_change'].rolling(window=4).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['volume_vol_8H'] = hour_aggs['volume_change'].rolling(window=8).apply(lambda x: abs(x).mean(), raw=True)
        hour_aggs['volume_vol_16H'] = hour_aggs['volume_change'].rolling(window=16).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_3D'] = daily_aggs['price_change'].rolling(window=3).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_5D'] = daily_aggs['price_change'].rolling(window=5).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_10D'] = daily_aggs['price_change'].rolling(window=10).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['return_vol_30D'] = daily_aggs['price_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_3D'] = daily_aggs['volume_change'].rolling(window=3).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_5D'] = daily_aggs['volume_change'].rolling(window=5).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_10D'] = daily_aggs['volume_change'].rolling(window=10).apply(lambda x: abs(x).mean(), raw=True)
        daily_aggs['volume_vol_30D'] = daily_aggs['volume_change'].rolling(window=30).apply(lambda x: abs(x).mean(), raw=True)

        min_aggs['15min_vol_diff'] = min_aggs['return_vol_15M'] - min_aggs['return_vol_240M']
        min_aggs['15min_vol_diff_pct'] = min_aggs['15min_vol_diff']/min_aggs['return_vol_240M']
        min_aggs['min_vol_diff'] = min_aggs['return_vol_30M'] - min_aggs['return_vol_240M']
        min_aggs['min_vol_diff_pct'] = min_aggs['min_vol_diff']/min_aggs['return_vol_240M']
        hour_aggs['hour_vol_diff'] = hour_aggs['return_vol_4H'] - hour_aggs['return_vol_16H']
        hour_aggs['hour_vol_diff_pct'] = hour_aggs['hour_vol_diff']/hour_aggs['return_vol_16H']
        daily_aggs['daily_vol_diff'] = daily_aggs['return_vol_3D'] - daily_aggs['return_vol_10D']
        daily_aggs['daily_vol_diff_pct'] = daily_aggs['daily_vol_diff']/daily_aggs['return_vol_10D']
        daily_aggs['daily_vol_diff30'] = daily_aggs['return_vol_3D'] - daily_aggs['return_vol_30D']
        daily_aggs['daily_vol_diff_pct30'] = daily_aggs['daily_vol_diff30']/daily_aggs['return_vol_30D']
        min_aggs['min_volume_vol_diff'] = min_aggs['volume_vol_240M'] - min_aggs['volume_vol_450M']
        min_aggs['min_volume_vol_diff_pct'] = min_aggs['min_volume_vol_diff']/min_aggs['volume_vol_450M']
        hour_aggs['hour_volume_vol_diff'] = hour_aggs['volume_vol_8H'] - hour_aggs['volume_vol_16H']
        hour_aggs['hour_volume_vol_diff_pct'] = hour_aggs['hour_volume_vol_diff']/hour_aggs['volume_vol_16H']
        daily_aggs['daily_volume_vol_diff'] = daily_aggs['volume_vol_3D'] - daily_aggs['volume_vol_10D']
        daily_aggs['daily_volume_vol_diff_pct'] = daily_aggs['daily_volume_vol_diff']/daily_aggs['volume_vol_10D']
        daily_aggs['daily_volume_vol_diff30'] = daily_aggs['volume_vol_3D'] - daily_aggs['volume_vol_30D']
        daily_aggs['daily_volume_vol_diff_pct30'] = daily_aggs['daily_volume_vol_diff30']/daily_aggs['volume_vol_30D']

        min_features = min_aggs.iloc[-1]
        hour_features = hour_aggs.iloc[-1]
        daily_features = daily_aggs.iloc[-1]
        min_features.drop(['t','o','h','l','v','c','price_change','volume_change','hour','minute','date'], inplace=True)
        hour_features.drop(['o','h','l','v','c','price_change','volume_change'], inplace=True)
        daily_features.drop(['o','h','l','v','c','price_change','volume_change'], inplace=True)
        df_combined = pd.concat([min_features, hour_features, daily_features])
        features.append(df_combined)
    
    features_df = pd.DataFrame(features)
    results_df = pd.merge(df, features_df, on=['symbol'], how='outer')
    return results_df

def call_polygon_histH(symbol_list, from_stamp, to_stamp, timespan, multiplier):
    payload={}
    headers = {}
    dfs = []
    error_list = []

    if timespan == "minute":
        from_stamp = to_stamp
    for symbol in symbol_list:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"

        response = execute_polygon_call(url)

        try:
            response_data = json.loads(response.text)
            results = response_data['results']
            results_df = pd.DataFrame(results)
            results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
            results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
            results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
            results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
            results_df['symbol'] = symbol
            trimmed_df = results_df.loc[results_df['hour'].isin([9,10,11,12,13,14,15])]
            filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
            dfs.append(filtered_df)
        except Exception as e:
            error_list.append(symbol)
            continue

    return dfs, error_list

def call_polygon_vol(symbol_list, from_stamp, to_stamp, timespan, multiplier,hour):
    payload={}
    headers = {}
    dfs = []
    trading_hours = [9,10,11,12,13,14,15]
    error_list = []

    year, month, day = to_stamp.split("-")
    current_date = datetime(int(year), int(month), int(day), int(hour),tzinfo=pytz.timezone('US/Eastern'))

    for symbol in symbol_list:
        data = []
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={KEY}"
        with requests.Session() as session:
            next_url = url
            while next_url:
                response = execute_polygon_call(next_url)
                try:
                    response_data = json.loads(response.text)
                    results = response_data['results']
                except:
                    error_list.append(symbol)
                    continue
                results_df = pd.DataFrame(results)
                results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
                results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
                results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
                results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
                results_df['symbol'] = symbol
                trimmed_df = results_df.loc[results_df['hour'].isin(trading_hours)]
                filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]
                filtered_df = filtered_df.loc[filtered_df['date'] <= current_date]
                data.append(filtered_df)
                try:
                    next_url = response_data['next_url']
                except:
                    next_url = None
            full_df = pd.concat(data, ignore_index=True)
            dfs.append(full_df)

    return dfs, error_list
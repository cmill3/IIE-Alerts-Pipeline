import json
from helpers.aws import pull_files_s3, get_s3_client
from helpers.data import calc_price_action
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from botocore.exceptions import ClientError

alerts_bucket = os.getenv("ALERTS_BUCKET")
prefixes = ["gainers","losers","most_actives","vdiff_gain"]
hours = [14,15]

logger = logging.getLogger()

def build_training_data(event, context, days_back):
    s3 = get_s3_client()
    date_str = build_date(days_back)
    for prefix in prefixes:
        for hour in hours:
            df = pull_alerts(s3, "inv-alerts", f"{prefix}/{date_str}/{hour}.csv")
            result = df.apply(calc_price_action, axis=1)
            price_df = pd.DataFrame(result.to_list())
            df = pd.merge(df, price_df, on="symbol")
            csv = df.to_csv()
            put_response = s3.put_object(Bucket="inv-alerts", Key=f"{prefix}/{date_str}/{hour}.csv", Body=csv)
    
    print(f"DONE with {date_str}!")

def build_date(days_back):
    today = datetime.now()
    date = today - timedelta(days=days_back)
    
    if date.day < 10:
        day = "0" + str(date.day)
    else:
        day = str(date.day)
        
    if date.month < 10:
        month = "0" + str(date.month)
    else:
        month = str(date.month)

    temp_year = date.year
    
    # date_dict ={
    #     'month': month,
    #     'day': day,
    #     'year': str(temp_year)
    #     }
    
    return f"{temp_year}/{month}/{day}"

def pull_alerts(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    data = pd.read_csv(response.get("Body"))
    return data


if __name__ == "__main__":
    build_training_data(None,None,14)   
    start_date = datetime(2023,8,16)
    end_date = datetime(2023,8,18)
    days = [0,1,2]
    for day in days:
        temp_date = start_date + timedelta(days=day)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            build_training_data(None,None,date_str)   
            

    # date_diff = end_date - start_date
    # numdays = date_diff.days 
    # date_list = []
    # print(numdays)
    # for x in range (0, numdays):
    #     temp_date = start_date + timedelta(days = x)
    #     if temp_date.weekday() < 5:
    #         # date_str = temp_date.strftime("%Y/%m/%d")
    #         date_list.append(temp_date)

    # for date in date_list:
    #     analytics_runner(None,None,date)
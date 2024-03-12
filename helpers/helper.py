import pandas as pd
import boto3
from datetime import datetime, timedelta

date = datetime.now()

def pull_model_config(trading_strategy):
    s3 = boto3.client('s3')
    weekday = date.weekday()
    monday = date - timedelta(days=weekday)
    date_prefix = monday.strftime("%Y/%m/%d")
    model_config = s3.get_object(Bucket="inv-alerts-trading-data", Key=f"model_configurations/{date_prefix}.csv")
    model_config = pd.read_csv(model_config.get("Body"))
    model_config = model_config.loc[model_config['strategy'] == trading_strategy]
    return {"target_value": model_config['target_value'].values[0], "strategy": model_config['strategy'].values[0]}

if __name__ == "__main__":
    print(pull_model_config("CDBFC"))
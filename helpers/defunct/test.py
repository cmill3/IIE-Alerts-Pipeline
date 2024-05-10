from datetime import datetime, timedelta
from helpers.data import *


if __name__ == "__main__":
    # import time 
    # dt = time.time()
    # df = pd.DataFrame()
    # build_date_dfs(df, dt)
    x = calculate_sellby_date(datetime.strptime("2024-04-15","%Y-%m-%d"),3)
    print(x)
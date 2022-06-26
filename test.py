# from mq.consumer import get_latest_data
import threading
from util import db_util
# from util.db_util import get_connection
from util.init_table import future_mapping, future_m
import jqdatasdk as jq
import pandas as pd
from datetime import datetime



def reset_table(start_date = "2022-06-21", end_date = datetime.now().strftime("%Y-%m-%d")):
    future_mapping(start_date=start_date, end_date=end_date)
    # future_m(
    #     start_date=start_date,
    #     end_date=end_date,
    #     skip_paused=True,
    # )

# def simulate(start_date = "2022-06-21", end_date = "2022-12-31"):
#     data = get_latest_data()
#     print('msg',data)
#     data: pd.DataFrame = jq.get_price(
#             security=['A2209.XDCE'],
#             start_date=start_date,
#             end_date=end_date,
#             frequency="minute",
#             skip_paused=True,
#             fields=[
#                 "open",
#                 "close",
#                 "low",
#                 "high",
#                 "volume",
#                 "money",
#                 "pre_close",
#                 "open_interest",
#             ],)
#     print(data)

if __name__ == '__main__':
    reset_table()
#    db = db_util.get_connection()
#    db.truncate('future_m')

"""
@File :get_data.py
@Author :Liulinxi
@Email :lkjfoli@163.com
@Date :2024/10/26
@Desc : 获取数据
"""

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from DB import connection
from model.tools import ProcessABC
from loguru import logger
import hashlib


def get_start_time():
    num_turbines = 982  # 风机数量，写死
    earliest_start_time = datetime(year=2020, month=1, day=1, hour=0)
    try:
        for turbine_id in range(1, num_turbines + 1):
            excel_name = f"../xlsx/turbine{turbine_id}.xlsx"
            df = pd.read_excel(excel_name)
            df['time'] = pd.to_datetime(df['time'])
            earliest_start_time = df['time'].max() if df['time'].max() > earliest_start_time else earliest_start_time
    except FileNotFoundError as e:
        pass
    return earliest_start_time


class GetData(ProcessABC):
    """
    获取数据对象
    """

    def __init__(self):
        self.end_time, self.start_time = None, None
        self.db_util = connection()

    def run(self, **kwargs):

        now_time = kwargs['params']
        # self.end_time = now_time
        # self.start_time = get_start_time()
        data = self.get_influx()
        return self.return_data(data=data, start_time=self.start_time, end_time=self.end_time)

    def get_influx(self):
        """
        获取influxdb数据
        :return: influxdb_data
        """

        query_filter = """
          |> filter(fn: (r) => r["_measurement"] == "FN01SW2012RAW")
        """
        try:
            influx = self.db_util.query_flux(bucket_name='time_series_data',
                                             start=self.start_time,
                                             end=self.end_time,
                                             filter=query_filter, return_type='df')
        except Exception as e:
            logger.warning(f"[GetData]实时数据获取失败: {e}")
            raise e
        # finally:
        #     logger.info(
        #         f"[GetData]在时间窗口{self.start_time}:{self.end_time}上查询到数据{influx.shape[0]}条.")

        return {'influx': influx}


if __name__ == "__main__":
    model = GetData()
    now_time = datetime.now()
    res = model.run(params=now_time)
    print(res)

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


class GetData(ProcessABC):
    """
    获取数据对象
    """

    def __init__(self):
        self.end_time, self.start_time = None, None
        self.db_util = connection()

    def run(self, **kwargs):

        now_time = kwargs['params']

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
            influx = self.db_util.query_flux(bucket_name='time_series_data2', import_name='regexp',
                                             start=self.start_time,
                                             end=self.end_time,
                                             filter=query_filter, return_type='df')
        except Exception as e:
            logger.warning(f"[GetData]实时数据获取失败: {e}")
            raise e
        finally:
            logger.info(
                f"[GetData]在时间窗口{self.start_time}:{self.end_time}上查询到数据{influx.shape[0]}条.")

        return {'influx': influx}

if __name__ == "__main__":

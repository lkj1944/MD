"""
@File :get_data.py
@Author :Liulinxi
@Email :lkjfoli@163.com
@Date :2024/10/26
@Desc : 获取数据
"""

import os
import zipfile
from datetime import datetime, timedelta, timezone
from os.path import exists
from re import match

import pandas as pd
from loguru import logger
from pandas import read_excel

from DB import connection
from model.tools import ProcessABC


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


def refresh_turbine_xlsx():
    folder_path = "../xlsx"
    file_names = os.listdir(folder_path)
    for file_name in file_names:
        file_path = folder_path + "/" + file_name
        sheet_name = file_name.replace(".xlsx","")
        try:
            df = read_excel(file_path)
            df_sorted = df.sort_values(by='time')
            with pd.ExcelWriter(file_path, engine='openpyxl', mode='w') as writer:
                df_sorted.to_excel(writer, index=False, sheet_name=sheet_name)
        except zipfile.BadZipfile as e:
            logger.error(f"梳理{file_path}文件时出现{e}报错")

class GetData(ProcessABC):
    """
    获取数据对象
    """

    def __init__(self):
        self.end_time, self.start_time = None, None
        self.db_util = connection()

    def run(self, **kwargs):
        now = kwargs['params']
        self.start_time = datetime(2024, 10, 31, 12, 0).replace(tzinfo=timezone(timedelta(hours=8)))
        self.end_time = datetime(2024, 10, 31, 12, 30).replace(tzinfo=timezone(timedelta(hours=8)))
        data = self.get_influx()
        return self.return_data(data=data,start=self.start_time,end=self.end_time)

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
        finally:
            logger.info(
                f"[GetData]在时间窗口{self.start_time}:{self.end_time}上查询到数据{influx.shape[0]}条.")
        influx['time'] = influx['time'].dt.floor('S')
        influx['_field'] = influx['_field'].astype(int)
        influx['_value'] = influx['_value'].astype(int)
        return {'influx': influx}


#Todo： 优化，删除refresh重新插入操作，优化FAD模型输出，写入到exl文件的同时输出df作为DA模型的输入

class FetchAlarmData(ProcessABC):
    def run(self, **kwargs):
        df = kwargs['params']['data']['influx']
        file_path = "xlsx"
        data = {}

        if os.path.exists(file_path):
            df = df.sort_values(by=['_field'], ascending=True)

            for field, group_df in df.groupby('_field'):
                sheet_name = f"turbine{field}"
                xlsx_path = file_path + f"/turbine{field}.xlsx"

                if os.path.exists(xlsx_path):
                    try:
                        with pd.ExcelWriter(xlsx_path, mode='a', engine='openpyxl',
                                            if_sheet_exists='overlay') as writer:
                            df_pre = pd.read_excel(xlsx_path)
                            for _, pre_row in df_pre.iterrows():
                                mask = (group_df['time'] - pre_row['time']).abs() < timedelta(seconds=1)
                                group_df = group_df[~mask]

                            combined_df = pd.concat([group_df, df_pre])
                            combined_df = combined_df.sort_values(by='time', ascending=True).reset_index(drop=True)
                            combined_df.to_excel(writer, index=False, sheet_name=sheet_name)
                            data[field] = combined_df
                    except zipfile.BadZipfile as e:
                        logger.error(f"查找{xlsx_path}文件时出现错误{e}")
                else:
                    with pd.ExcelWriter(xlsx_path, mode='w', engine='openpyxl') as writer:
                        group_df = group_df.sort_values(by='time', ascending=True).reset_index(drop=True)
                        group_df.to_excel(writer, index=False, sheet_name=sheet_name)
                        data[field] = group_df
        else:
            logger.error(f"[FetchAlarmData]找不到xlsx文件夹")

        combined_data_df = pd.concat(data.values()).reset_index(drop=True)

        return self.return_data(data=combined_data_df)


if __name__ == "__main__":
    model = GetData()
    now_time = datetime.now()
    res = model.run(params=now_time)
    model1 = FetchAlarmData()
    model1.run(params=res)
    print(res)
    print(now_time, datetime.now())

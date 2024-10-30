from datetime import datetime, timedelta
import time
import pytz
import pandas as pd

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "time_series_data2"

client = InfluxDBClient(url="http://120.92.122.212:18087",
                        token="FI_3BldQwibDfscvVIDeTk4QYipkQUChaX-wza2jzVDa_91S"
                              "-3aGWL0wYsxhehgHDc7e6bPhhchfKIvDeQ0DFw==",
                        org="data")

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

start_time = datetime.now(pytz.utc)
try:
    query = """
from(bucket: "time_series_data")
  |> range(start: 2020-01-01 00:00:00, stop: now())
  |> filter(fn: (r) => r["_measurement"] == "FN01SW2012RAW")
    """
    query_tables = query_api.query(query)

    # 准备存储查询结果的列表
    data = []

    # 遍历查询到的表格
    for _table in query_tables:
        for record in _table.records:
            # 将记录的每一行数据存储到字典中
            row = {
                'time': record.get_time(),  # 时间戳
                '_value': record.get_value(),  # 数据值
                '_field': record.values['_field'],  # 字段名称
                '_measurement': record.values['_measurement']  # 测量名称
            }
            data.append(row)

    # 将数据转换为DataFrame
    df = pd.DataFrame(data)
    df['time'] = df['time'].dt.tz_convert('Asia/Shanghai')
    df['time'] = df['time'].dt.tz_localize(None)
    df.to_excel('output.xlsx', index=False)
    #print(df)

except Exception as e:
    print(f"Error: {e}")
#
# while 1:
#     print(1)

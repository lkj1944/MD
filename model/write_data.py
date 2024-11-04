import math
# -*- coding: utf-8 -*-
import re

from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from model.tools import ProcessABC
from model.data_analysis import DataAnalysis

from DB import connection
from influxdb_client import WritePrecision, Point

# Todo : Writing speed optimization needed, freaking 1000 pieces of data need 200s to insert in the database
class WriteRes(ProcessABC):
    """
    结果写入主对象
    """

    def __init__(self):
        """
        实例化该对象属性
        由于回写的均是回写到mysql数据库中，所以暂不考虑influx连接作用，主要使用方法是execute_dml方法
        利用该方法提交事务并进行提交
        """
        self.db_util = connection()


    def run(self, **kwargs):
        """
        对外暴露方法，用于处理整个流程，一切基于对象本身属性进行修改
        :return:
        """
        now1 = datetime.now()
        self.write_alarm_res(kwargs['params']['data'][0])
        now2 = datetime.now()
        logger.info(f"写入报警数据花费的时间为{now2-now1}")
        self.write_health_res(kwargs['params']['data'][1])
        now3 = datetime.now()
        logger.info(f"写入健康度花费的时间为{now3-now2}")
        return self.return_data(data=kwargs['params']['data'])

    def write_alarm_res(self, output_data):
        output_data.rename(columns={'_field': 'fan_id', '_status': "block_type1", 'end_time': 'stop_time'},
                           inplace=True)

        output_data['stop_time'] = pd.to_datetime(output_data['stop_time'], errors='coerce')
        output_data['stop_time'] = output_data['stop_time'].apply(lambda x: None if pd.isna(x) else x)

        output_data.drop(columns=['_value'], inplace=True)
        output_data['create_time'] = datetime.now()

        field = output_data.columns.tolist()
        values = output_data.values.tolist()
        # self.write_res(values, field, 't_start_stop')

    def write_health_res(self, output_data):
        output_data['update_time'] = datetime.now()

        fields_to_update = ['score', 'update_time']

        for _, row in output_data.iterrows():
            # 获取要更新的值列表
            values_to_update = [row['score'], row['update_time']]
            # 获取 fan_id 作为条件
            fan_id = row['fan_id']

            # 调用 update 方法
            self.db_util.update_sql(
                table='intelligent_perception_fan_health',
                fields=fields_to_update,
                values=values_to_update,
                condition_field='fan_id',
                condition_value=fan_id
            )

    def write_res(self, wait_write, field, table):
        """
        将结果输出到sql，二开self.put_sql
        :return:
        """
        for i in wait_write:
            self.db_util.put_sql(table=table, fields=field, values=i)

    def change_time(self):
        """
        将时间戳更改为开始时间与结束时间，并把时间戳从utc时间更改成为中国时间
        :return:
        """
        time_stamp = self.res_all['TIMESTAMP']
        time_list = [i.replace("Z", '') for i in time_stamp.split("Z-")]
        self.start_time = (
                datetime.strptime(time_list[0], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=8)
        ).strftime("%Y-%m-%d %H:%M:%S")
        self.end_time = (
                datetime.strptime(time_list[1], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=8)
        ).strftime("%Y-%m-%d %H:%M:%S")

    def change_alarm_res(self):
        """
        更改异常结果的数据结构
        原结构：
        alarm_code: {fan_id: {传感器名称:{alarm_level: string, alarm_value: float, threshold_value: float, fault_code: string}}}
        转换结构：
        [fan_code, fan_name, category, rank, start_time, end_time, param, senaor_num, detn_val, the_val, describe, code, create_time, duration, alarm_type, fault_code]
        :return:
        """
        alarm_res = list()
        for code, fan_dict in self.res_all['ALARM_RES'].items():
            for fan_id, error_dict in fan_dict.items():
                for error_place, error_info in error_dict.items():
                    # 如果存在则添加相应的值
                    if error_info:
                        now_res = list()
                        # fan_id
                        fan_idx = self.fan_res['code'].index(fan_id)
                        fan_res_id = self.fan_res['id'][fan_idx]
                        now_res.append(fan_res_id)
                        # 添加fan_code，为风机名称，与fan_name一样
                        now_res.append(fan_id)
                        # 添加fan_name
                        now_res.append(self.fan_res['name'][fan_idx])
                        # 添加category，需要根据映射添加
                        now_res.append(self.category[code[:2]])
                        # 添加rank
                        now_res.append(self.level[error_info['alarm_level']])
                        # 添加start_time
                        start_time = self.start_time
                        now_res.append(start_time)
                        # 添加end_time
                        end_time = None
                        now_res.append(end_time)
                        # 添加param
                        param_code = code[:2]
                        if param_code == "01":
                            now_res.append(self.param['01'][code])
                        else:
                            now_res.append(self.param[param_code])
                        # 添加sensor_num
                        if 'RD_' in error_place:
                            now_res.append(error_place)
                        elif re.search(r"CL[0-9]{2}-[0-9]X|Y", error_place) or \
                                re.search(r"AT[0-9]{2}-[0-9]X|Y", error_place):
                            split_list = error_place.split("-")
                            now_res.append(f"{split_list[0][:2]}{fan_id[-2:]}-{split_list[1]}轴")
                        else:
                            mapping_res = self.serson.get(error_place, "")
                            if mapping_res:
                                mapping_split = mapping_res.split("-")
                                mapping_res = f"{mapping_split[0]}{fan_id[-2:]}-{mapping_split[1]}"
                            now_res.append(mapping_res)
                        # 添加detn_val
                        now_res.append(float(error_info['alarm_value']))
                        # 添加the_val
                        now_res.append(error_info['threshold_value'])
                        # 添加describe
                        describe = self.describe.loc[(self.describe['code'] == code) & (
                                self.describe['alarm_type'] == error_info['alarm_level']),
                        'waring_desc'].values[0] if error_info['alarm_level'] != '04' else None
                        now_res.append(describe)
                        # 添加code
                        now_res.append(code)
                        # 添加create_time
                        now_res.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S.000'))
                        # 添加duration
                        now_res.append(
                            (datetime.strptime(self.end_time, '%Y-%m-%d %H:%M:%S') - datetime.strptime(start_time,
                                                                                                       '%Y-%m-%d %H:%M:%S')).total_seconds()
                        )
                        # 添加alarm_type
                        now_res.append(error_info['alarm_level'])
                        # 添加fault_code
                        fault_code = error_info['fault_code'] if 'fault_code' in error_info.keys() else None
                        now_res.append(fault_code)
                        # 添加到整体列表中
                        alarm_res.append(now_res)
        return alarm_res

    def creat_category_temp(self):
        """
        创建测点所属大类的映射
        :return:
        """
        return {
            "01": "环境",
            "02": "风机",
            "03": "风机",
            "04": "负压筒",
            "05": "负压筒",
            "06": "负压筒",
            "07": "负压筒"
        }

    def creat_level_temp(self):
        """
        创建预警级别编码映射
        :return:
        """
        return {
            "01": "预警",
            "02": "报警",
            "03": "紧急报警",
            "04": "正常"
        }

    def creat_param_temp(self):
        """
        创建监测点类别映射
        :return:
        """
        return {
            "01": {"0101": "风速", "0102": "最大波高", "0103": "有效波高", "0104": "潮位"},
            "02": "倾斜",
            "03": "振动",
            "04": "螺栓应力",
            "05": "钢板应力",
            "06": "土压力",
            "07": "孔隙水压力"
        }

    def creat_describe(self):
        """
        获取信息描述表相关信息
        :return:
        """
        sql = """
        SELECT code, alarm_type, waring_desc FROM t_describe
        """
        self.describe = self.db_util.execute_sql(sql=sql, return_type='df')

    def get_sensor(self):
        """
        获取传感器信息表
        :return:
        """
        sql = """
        SELECT sensor_name, alarm_name FROM t_sensor
        """
        serson_res = self.db_util.execute_sql(sql=sql)
        self.serson = dict(zip(serson_res['alarm_name'], serson_res['sensor_name']))

    def get_fan_id(self):
        """
        获取fan_id相关数据
        :return:
        """
        sql = """
        SELECT id, name, code FROM t_fan
        """
        fan_res = self.db_util.execute_sql(sql=sql)
        fan_res['id'].append(0)
        fan_res['name'].append('环境')
        fan_res['code'].append('00')
        return fan_res

    def change_curve_res(self, start_time=None):
        """
        曲线类数据格式转换
        :param start_time:
        :return:
        """
        curve_res = []
        for i in range(len(self.res_all['CURVE_RES'])):
            if start_time:
                curve_res.append(
                    Point(self.res_all['CURVE_RES'][i][1]).field(self.res_all['CURVE_RES'][i][0],
                                                                 self.res_all['CURVE_RES'][i][2]).time(
                        (datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") - timedelta(hours=8)), WritePrecision.NS))
            else:
                curve_res.append(
                    Point(self.res_all['CURVE_RES'][i][1]).field(self.res_all['CURVE_RES'][i][0],
                                                                 self.res_all['CURVE_RES'][i][2]).time(
                        self.res_all['CURVE_RES'][i][3], WritePrecision.NS))
        return curve_res


if __name__ == "__main__":
    model = DataAnalysis()
    res = model.run()
    write_model = WriteRes()
    write_res = write_model.run(params=res)
    print(f"写入到数据库成功")
    print(write_res)

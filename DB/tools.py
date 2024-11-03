"""
@File :client_base.py
@Author:MicClengxiang
@Emain: 1668362496@qq.com
@Date :2022/12/519:33
@Desc : 数据库初始化连接
"""

import os
import pymysql
import warnings
import configparser
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from functools import wraps
from loguru import logger
import time

from pandas import NaT


def retry(max_attempts=3, delay=0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.opt(depth=1).warning(f"{func.__name__}第{attempts + 1}次重试失败, 异常为: {e}")
                    attempts += 1
                    time.sleep(delay * attempts)
                    if attempts == max_attempts:
                        logger.opt(depth=1).error(f"{func.__name__}在{max_attempts}次重试后失败.")
                        raise e

        return wrapper

    return decorator


if '/' in os.path.abspath('.'):
    GLOBAL_MYSQL = 'MySQLScene'
    GLOBAL_INFLU = 'InfluxDBScene'
else:
    GLOBAL_MYSQL = 'MySQLHome'
    GLOBAL_INFLU = 'InfluxDBHome'


# 去除全局警告信息
# warnings.simplefilter("ignore", MissingPivotFunction)
class GetClient:
    """
    继承inflex与mysql
    """

    def __init__(self, cfg_name='db_config.ini'):
        """
        初始化连接信息，该初始化后该对象将存在mysql与influx的连接与游标
        :param cfg_name: 配置文件名称
        """
        config = self.get_cfg(cfg_name)
        self.client, self.cursor = self.get_client(config)
        # 获取连接资源
        self.flux_client = self.get_flux_client(config)
        # 实例influx查询api接口
        self.query_api = self.flux_client.query_api()
        # 实例写数api接口
        self.write_api = self.flux_client.write_api(write_options=SYNCHRONOUS)

    @retry(max_attempts=5, delay=1)
    def write(self, bucket, record):
        return self.write_api.write(bucket=bucket, record=record)

    @staticmethod
    @retry(max_attempts=5, delay=1)
    def get_flux_client(config):
        # 利用官方自带的token进行建立连接
        client = InfluxDBClient(
            url=str(config.get(GLOBAL_INFLU, 'url')),
            token=str(config.get(GLOBAL_INFLU, 'token')),
            org=str(config.get(GLOBAL_INFLU, 'org')),
            timeout=eval(config.get(GLOBAL_INFLU, 'timeout'))
        )
        return client

    @staticmethod
    def get_client(config):
        """
        获取连接以及游标
        :return: 连接与游标
        """
        conn = pymysql.connect(
            host=str(config.get(GLOBAL_MYSQL, 'ip')),
            port=int(config.get(GLOBAL_MYSQL, 'port')),
            user=str(config.get(GLOBAL_MYSQL, 'uname')),
            password=str(config.get(GLOBAL_MYSQL, 'password')),
            charset=str(config.get(GLOBAL_MYSQL, 'charset')),
            db=str(config.get(GLOBAL_MYSQL, 'database'))
        )
        cursor = conn.cursor()
        return conn, cursor

    @staticmethod
    def get_cfg(cfg_name):
        """
        寻找config名称方法
        :param cfg_name: 配置文件名称
        :return: 读取后的配置文件
        """
        db_cfg = configparser.ConfigParser()
        abs_path = os.path.join(os.path.dirname(__file__), cfg_name)
        db_cfg.read(abs_path, encoding='UTF-8')
        return db_cfg


class connection(GetClient):
    """
    对外暴漏对象，并二开mysql与influx方法
    """

    def __init__(self):
        """
        构造方法
        """
        super(connection, self).__init__()

    def mysql_close(self):
        """
        关闭游标与连接
        :return:
        """
        if self.client:
            self.client.close()
        if self.cursor:
            self.cursor.close()

    def execute_dml(self, sql, args=None):
        """
        operation DML statement
        :param sql: DML statement
        :param args: statement params
        :return: statement in to mysql
        """
        try:
            self.cursor.execute(sql, args)
            self.client.commit()
        except Exception as e:
            logger.error(e)
            logger.error(f"sql as: {sql}, args as: {args}")
            if self.client:
                self.client.rollback()

    def execute_sql(self, sql, args=tuple(), nums=-1, return_type='key_value'):
        """
        operation DQL statement
        :param return_type: return type as owner definition
        :param sql: DQL statement
        :param args: statement params
        :param nums: get sql number
        :param return_type: get result type
        1.not: 传递not或其他值时返回原始查询信息
        2.key_value: 返回经过解析后查询的信息，格式为{'查询字段名称': [所有值]}
        3.set： 返回的结果通过key_value的形式并进行去重操作
        4.df: key_value延申模式，将返回格式dataframe化
        :return: statement in nums
        """
        self.cursor.execute(sql, args)
        # 获取查询结果个数并进行返回
        if nums == -1:
            sql_res = self.cursor.fetchall()
        else:
            sql_res = self.cursor.fetchmany(nums)
        # 解析结果
        if return_type == 'key_value' or return_type.lower() == 'df':
            res_dict = {}
            # get query field
            split_from = sql.split('FROM')[0] if 'FROM' in sql else sql.split('from')[0]
            fields = split_from.replace('SELECT', '') if 'SELECT' in split_from else split_from.replace('select', '')
            if fields.strip() == '*':
                field_list = [des[0] for des in self.cursor.description]
            else:
                field_list = fields.strip().split(',')
                field_list = [i.strip() for i in field_list]
                # 清洗含有AS的名称
                for i in range(len(field_list)):
                    if ' AS ' in field_list[i]:
                        field_list[i] = field_list[i].split(' AS ')[1].strip()
                    elif ' as ' in field_list[i]:
                        field_list[i] = field_list[i].split(' as ')[1].strip()
            # 清洗没带别名但是连表查询的字段
            for i in range(len(field_list)):
                if '.' in field_list[i]:
                    field_list[i] = field_list[i].split('.')[1]
            # 拼接字典
            if sql_res:
                for res_num in range(len(sql_res[0])):
                    if return_type == 'set':
                        res_dict[field_list[res_num]] = list(set([i[res_num] for i in sql_res]))
                    else:
                        res_dict[field_list[res_num]] = [i[res_num] for i in sql_res]
            # df对应
            if return_type.lower() == 'df':
                return pd.DataFrame(res_dict)
        else:
            return sql_res
        return res_dict

    @retry(max_attempts=5, delay=0)
    def query_flux(self, bucket_name, start=None, end=None, filter=None, last_time='-5m', limit=None, import_name=None,
                   sort=False, return_type='original'):
        """
        给influxdb写的二开接口，主要是为了简介开发查询语句
        :param last_time: 按照时间前推获取
        :param bucket_name: 库名称
        :param start: 开始时间
        :param end: 结束时间
        :param filter: 是否有自增的判断条件，如果没有则是查询全部
        :param limit: 数据限制条数limit
        :param import_name: influx查询时引入报名
        :param return_type: 返回类型
        :param sort: 是否按时间倒序返回，默认为否
        1.original: 原始数据，从influxdb查出来是什么样子就直接返回什么样子，具体查看官方文档
        2.df: pandas.DataFrame格式，通过原始数据查询出来的结果进行解析为df
        3.
        :return:
        """
        import_sql = f'import "{import_name}"'
        flux_sql = import_sql + f'from(bucket: "{bucket_name}")' if import_name else f'from(bucket: "{bucket_name}")'
        if start and end:
            flux_sql += f'|>range(start: time(v:"{start.isoformat()}"), stop: time(v:"{end.isoformat()}"))'
        else:
            flux_sql += f'|>range(start: {last_time}, stop:now())'
        if filter:
            for i in filter:
                flux_sql = flux_sql + i
        if sort:
            flux_sql += f'|>sort(columns: ["_time"], desc: true)'
        if limit:
            flux_sql += f'|>limit(n: {limit})'
        logger.opt(depth=2).info(f"当前查询的fluxql为: {flux_sql}")
        # if return_type.lower() == 'df':
        tables = self.query_api.query(flux_sql)
        data = []
        for _table in tables:
            for record in _table.records:
                row = {
                    'time': record.get_time(),
                    '_value': record.get_value(),
                    '_field': record.values['_field'],
                    '_measurement': record.values['_measurement']
                }
                data.append(row)
        df = pd.DataFrame(data)
        df['time'] = df['time'].dt.tz_convert('Asia/Shanghai')
        df['time'] = df['time'].dt.tz_localize(None)
        # else:
        #     tables = self.query_api.query(flux_sql)
        if return_type.lower() == 'own_df':
            """
            该变量对应own_df的话代表返回自定义的dataframe
            """
            pass
        # 因为单次查询数量超过36W条会分页，因此在这里合并
        # if isinstance(tables, list):
        #     res_df = pd.DataFrame([])
        #     for table in tables:
        #         res_df = pd.concat([res_df, table])
        # else:
        res_df = df
        if res_df.empty:
            raise Exception(f"当前查询的fluxql为: {flux_sql}, 但是查询结果为空.")
        return res_df

    def put_sql(self, table, fields, values):
        """
        回写sql的方法，主要是为了满足动态INSERT数据操作，二开execute_dml的insert操作
        :param table: 表名
        :param fields: 字段列表
        :param values: 值列表
        :return:
        """
        fields = ', '.join(fields)
        args_str = ', '.join(['%s'] * len(values))
        sql_dml = f'INSERT INTO {table}' \
                  f'({fields})' \
                  f'VALUES' \
                  f'({args_str})'
        if values[1] is NaT:
            values[1] = None
        self.execute_dml(sql=sql_dml, args=tuple(values))


if __name__ == '__main__':
    get = connection()
    print(get.flux_client)
    print(get.cursor)

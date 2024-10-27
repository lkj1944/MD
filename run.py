"""
@Date
@Desc
@File
"""
import os.path
from time import time
from datetime import datetime, timedelta
import sqlite3
from loguru import logger
import apscheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import argparse
import model
from model.cfg import RETRY_DELAY

scheduler = BlockingScheduler()

executor = ProcessPoolExecutor()
scheduler.add_executor(executor)
logger.debug("任务调度器初始化完成")


class Faced(object):
    """
    外观设计模式对象，将类名字符串进行实例化操作
    """

    @staticmethod
    def create_flow(process):
        return getattr(model, process)()


def run(flow, **kwargs):
    """
    :param flow:
    :param kwargs:
    :return:
    """

    params = kwargs['now_time']
    try:
        time_start_all = time()
        for cls in flow:
            time_start_func = time()
            attr = Faced.create_flow(cls)
            params = attr.run(params=params)
            time_end_func = time()
            logger.info(f"{cls}.run()执行时长：{time_end_func - time_start_func}")
        time_end_all = time()
        logger.info(f"{flow}执行时长：{time_end_all - time_start_all}")
    except Exception as e:
        logger.error(f"flow {cls} failed with exception:{e}, runtime parameter is {params}")
        raise e


def main(**kwargs):
    """
    主流程入口
    :param kwargs:
    :return:
    """
    now_time = datetime.now()
    flow = ['GetData', 'FetchAlarmData', 'DataAnalysis', 'WriteAggAlarmData']
    run(flow, now_time=now_time)


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-d', action='store_true', help='Enable debug mode')
    # parser.add_argument('--class_name', nargs='+', help='Class names')
    # parser.add_argument('--start_time', help='Start time')
    # parser.add_argument('--end_time', help='End time')
    scheduler.add_job(
        main,
        CronTrigger(minute='*/10'),  # 每五分钟执行一次
        id='alarm_data_process'
    )
    scheduler.start()
    main()

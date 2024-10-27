import os.path
from time import time
from datetime import datetime, timedelta
import sqlite3
from loguru import logger
import apscheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import argparse
import model
from model.cfg import RETRY_DELAY

scheduler = BlockingScheduler()

executor = ProcessPoolExecutor()
scheduler.add_executor(executor)


def print_per_min(**kwargs):
    print_string = kwargs['str']
    print(f"每1分钟输出{print_string}.")


def print_every_five_second(**kwargs):
    print_char = datetime.now()
    print(f"每5秒输出{print_char}.")


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-d', action='store_true', help='Enable debug mode')
    # parser.add_argument('--class_name', nargs='+', help='Class names')
    # parser.add_argument('--start_time', help='Start time')
    # parser.add_argument('--end_time', help='End time')
    scheduler.add_job(
        print_per_min,
        CronTrigger(minute='*'),  # 每分钟执行一次
        kwargs={
            "str": "你好"
        },
        id='agg_alarm'
    )

    scheduler.add_job(
        print_every_five_second,
        CronTrigger(second='*/5'),  # 每五秒执行一次
        id='write_alarm'
    )
    scheduler.start()

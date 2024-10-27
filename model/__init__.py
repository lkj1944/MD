from loguru import logger
import logging

import json
import os
import sys

cfg_path = 'config/project_config.json'
with open(cfg_path) as f:
    global_config = json.load(f)


def init_logger():
    logger_config = global_config['logger']
    logger.remove()
    logger.add(
        sink=os.path.join(
            logger_config['log_file_path'].format(
                root=os.getcwd()),
            logger_config['log_file_name']
        ),
        format=logger_config['log_format'],
        level=logger_config[f"{global_config['mode']}_log_level"],
        **logger_config['log_file_settings']
    )
    logger.add(
        sink=sys.stderr,
        format=logger_config['log_format'],
        level=logger_config[f"{global_config['mode']}_log_level"],
    )
    logging.getLogger("apscheduler").setLevel(logging.DEBUG)
    logger.debug("Logger initialized.")


def init():
    init_logger()


init()
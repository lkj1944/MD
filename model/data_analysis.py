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



class DataAnalysis(ProcessABC):
    def run(self):
        for



if __name__ == "__main__":
    pass
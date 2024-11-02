import os
import json

import pandas as pd
from pandas import read_excel

from model.tools import ProcessABC


class DataAnalysis(ProcessABC):
    def run(self, **kwargs):
        ap_model = AlarmProcess()
        ap_res = ap_model.run(data=kwargs['params'])
        # HS_model = HealthScore()
        # HS_res = HS_model.run()
        return self.return_data(data=ap_res['data'])

    def data_combination(self, data):
        df = data
        df['group'] = (df['_value'] != df['_value'].shift()).cumsum()
        result = df.groupby('group').agg(
            start_time=('time', 'first'),
            end_time=('time', 'last'),
            _value=('_value', 'first'),
            _field=('_field', 'first')
        ).reset_index(drop=True)
        return self.return_data(data=result)


class AlarmProcess(DataAnalysis):
    def __init__(self):
        self.type_lookup = []
        self.turbine_type_map = []
        with open('turbine_data.json', 'r') as f:
            data = json.load(f)
            self.type_lookup = data['type_lookup']
            self.turbine_type_map = data['turbine_type_map']

    def run(self, **kwargs):
        df = self.data_combination(kwargs['data'])['data']
        df['block_type1'] = df.apply(lambda row: self.get_status_code(row['_field'], row['_value']), axis=1)
        df = df[df['block_type1'].notna()]
        return self.return_data(data=df)

    def get_status_code(self, turbine_id, alarm_code):
        turbine_key = f"turbine{int(turbine_id):03}"
        # 获取风机类型
        turbine_type = self.turbine_type_map.get(turbine_key)
        if not turbine_type:
            return None
        status_code = self.type_lookup.get(turbine_type, {}).get(str(alarm_code))
        return status_code


class HealthScore(DataAnalysis):
    def run(self, **kwargs):
        return 1


if __name__ == "__main__":
    model = DataAnalysis()
    res = model.run()
    print(res)

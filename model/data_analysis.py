import os
import json
from datetime import datetime
from sre_constants import error
from venv import logger

import numpy as np
import pandas as pd
from pandas import read_excel, Index

from model.tools import ProcessABC


class DataAnalysis(ProcessABC):
    def run(self, **kwargs):
        ap_model = AlarmProcess()
        ap_res = ap_model.run(data=kwargs['params'])
        HS_model = HealthScore()
        HS_res = HS_model.run(data=kwargs['params'])
        return self.return_data(data=[ap_res['data'],HS_res['data']])

    def data_combination(self, data):
        df = data['data']
        result = []
        for _, sub_df in df.groupby('_field'):
            # 对每个分组应用分段和聚合操作
            sub_df['group'] = (sub_df['_value'] != sub_df['_value'].shift()).cumsum()
            temp_result = sub_df.groupby('group').agg(
                start_time=('time', 'first'),
                end_time=('time', 'last'),
                _value=('_value', 'first'),
                _field=('_field', 'first')
            )
            grouped_results = [subb_df for _, subb_df in temp_result.groupby('group')]
            for i in range(len(grouped_results)):
                grouped_results[i] = grouped_results[i].copy()
                try:
                    grouped_results[i]['end_time'] = grouped_results[i + 1]['start_time'].values[0]
                    pass
                except IndexError:
                    grouped_results[i]['end_time'] = None
            final_grouped_result = pd.concat(grouped_results, ignore_index=True)
            result.append(final_grouped_result)

        final_result = pd.concat(result, ignore_index=True)
        return self.return_data(data=final_result)


class AlarmProcess(DataAnalysis):
    def __init__(self):
        self.type_lookup = []
        self.turbine_type_map = []
        self.health_score_formula = []
        with open('model/turbine_data.json', 'r') as f:
            data = json.load(f)
            self.type_lookup = data['type_lookup']
            self.turbine_type_map = data['turbine_type_map']
            self.health_score_formula = data['health_score']

    def run(self, **kwargs):
        df = self.data_combination(kwargs['data'])['data']
        df['block_type1'] = df.apply(lambda row: self.get_status_code(row['_field'], row['_value']), axis=1)
        df = df[df['block_type1'].notna()]
        return self.return_data(data=df)

    def get_status_code(self, turbine_id, alarm_code):
        turbine_key = f"turbine{int(turbine_id):03}"
        turbine_type = self.turbine_type_map.get(turbine_key)
        if not turbine_type:
            return None
        status_code = self.type_lookup.get(turbine_type, {}).get(str(alarm_code))
        return status_code


class HealthScore(AlarmProcess):

    def run(self, **kwargs):
        df = kwargs['data']['data']
        df['block_type1'] = df.apply(lambda row: self.get_status_code(row['_field'], row['_value']), axis=1)
        health_scores = df.groupby('_field').apply(self.calculate_health_score).reset_index(name='score')

        health_scores.rename(columns={'_field': 'fan_id'}, inplace=True)

        return self.return_data(data=health_scores)

    def calculate_health_score(self, df):
        block_counts = df['block_type1'].value_counts(normalize=True)

        health_value = sum(
            block_counts.get(block, 0) * self.health_score_formula.get(block, 0) for block in block_counts.index)

        final_health_score = 100 - health_value
        return final_health_score



if __name__ == "__main__":
    model = DataAnalysis()
    res = model.run()
    print(res)

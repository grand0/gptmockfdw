from multicorn import ForeignDataWrapper

import requests
import json
import csv
import time


DEFAULT_BASE_URL = 'https://api.openai.com/v1/chat/completions'
DEFAULT_MODEL = 'gpt-3.5-turbo'
DEFAULT_QUERY = "Generate a fake data based on columns that you've been supplied with."
DEFAULT_TEMP = 0.1


class GptMockFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(GptMockFdw, self).__init__(options, columns)
        self.access_token = options['access_token']
        self.base_url = options.get('base_url', DEFAULT_BASE_URL)
        self.model = options.get('model', DEFAULT_MODEL)

        self.columns = []
        for col_name, col in columns.items():
            if col_name != 'gpt_query':
                self.columns.append(col_name)

        columns_str = ', '.join(self.columns)
        self.system_query = f"I want you to act as a fake data generator. User needs a dataset that has {len(self.columns)} columns: {columns_str}. Write your response in CSV format. DO NOT include a row with column names. User may provide additional details about what data thay want. If user haven't provided count of rows, generate 5 rows by default. Respond with just a data in the form of CSV, nothing else."

    def execute(self, quals, columns):
        query = DEFAULT_QUERY
        for qual in quals:
            if qual.field_name == 'gpt_query' and qual.operator == '=':
                query = qual.value

        body = json.dumps({
            'model': self.model,
            'messages': [{'role': 'system', 'content': self.system_query}, {'role': 'user', 'content': query}],
            'temperature': DEFAULT_TEMP
        })
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Charset': 'utf-8',
            'Authorization': f'Bearer {self.access_token}'
        }
        r = requests.request('POST', self.base_url, data=body, headers=headers)
        j = r.json()
        
        try:
            csv_str = j['choices'][0]['message']['content'].strip(" \n`")
            csv_str_rows = csv_str.split("\n")
            csv_list = csv.reader(csv_str_rows)
            rows = []
            for csv_row in csv_list:
                row = dict(zip(self.columns, csv_row))
                row['gpt_query'] = query
                rows.append(row)
            return rows
        except:
            with open(f'/tmp/gptmockfdw_err_{int(time.time())}.log', 'w') as f:
                f.write(str(j))
                f.flush()
            return []

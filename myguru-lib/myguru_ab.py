import sys
import yaml
import pandas as pd

'''
Модуль для AB-тестирования
'''

def get_config(file='myguru-lib/config.yml'):
    with open(file, 'r') as file:
        config = yaml.safe_load(file)
        CONFIG = config
        return config

this = sys.modules[__name__]
this.config=get_config()


def get_active_questions_df():
    df = pd.DataFrame(this.config['ab_questions'])

    exclude_ids = this.config['ab_question_ids_disable']
    df = df[~df.id.isin(exclude_ids)]

    label_field = this.config['ab_question_label_type']
    df = df.rename(columns={label_field: "label"})

    return df

#!/usr/bin/env python
from sklearn.ensemble import HistGradientBoostingClassifier
import pandas as pd

test_data = {
    'user_phone': ''
    'group_id': 3,
    'question_id': 5,
    'confirm': 1
}

# get_user_info_by_phone depends on environment
user_info = get_user_info_by_phone(test_data[user_phone]);
user_data = test_data + user_info

param_to_predict='result'

### CREATE NEURO
# in production you need just load already fited model object

calls_df = pd.read_csv( '1.csv', sep=',', encoding='utf-8' )
calls_df['phone'] = calls_df['phone'].astype("string")

users_df = pd.read_sql(
        'SELECT user_phone, id, '
        + ', '.join(ml_fields_user)
        + ', '
        + ', '.join(ml_fields_user_stat)
        + ' FROM user LEFT JOIN users_statictic ON user.id = users_statictic.user_id'
        + ' WHERE user_phone IN ('
        + ', '.join(calls_df['phone'].tolist())
        + ')',
        'mysql+pymysql://pavel:rL0eW8pC7ekX7z@185.149.241.75/crm'
)
users_df.rename(columns = {'user_phone':'phone'}, inplace=True)

df = pd.merge(calls_df, users_df, how='left', on='phone')

y = df[param_to_predict]
X = df.loc[:, df.columns != param_to_predict]
model_gb = HistGradientBoostingClassifier().fit(X, y)

### END OF LOAD NEURO

result = model_gb .predict( pd.DataFrame( data = user_info, index = [0] ) )
print(result[0])

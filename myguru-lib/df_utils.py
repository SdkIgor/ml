import pandas as pd

'''
Общие утилиты для работы с объектами pandas.DataFrame
(в основном для Exploratory Data Analysis)

'''


# # TODO
# def autofix_offer(df):
#     '''
#     Применяет следующие автофиксы
#     1) convert_dtypes()
#     2) если nunique = 1 и percent_missing = 0 - отправляет в список плохих колонок
#     3) если nunique = total - отправляет в список плохих колонок
#     3) если nunique = 1 и percent_missing != 0 - заменяет поле на has_<оригинальное_имя_поля> и меняет типа на bool
#     4) если nunique = 2 и percent_missing = 0 - то же самое (заменяет поле на has_<оригинальное_имя_поля> и меняет типа на bool)
#     5) если diversity больше cluster_treshhold и тип = строка (после .convert_dtypes())- рекомендуем fixdict
#     TO-DO: можно ещё проанализировать ntlk
#     '''
#     return 1


def diversity_percentage(df, columns):
    """
    This function returns the number of different elements in each column as a percentage of the total elements in the group.
    A low value indicates there are many repeated elements.
    Example 1: a value of 0 indicates all values are the same.
    Example 2: a value of 100 indicates all values are different.
    """
    diversity = dict()

    for col in columns:
        diversity[col] = len(df[col].unique())

    diversity_series = pd.Series(diversity)
    return (100*diversity_series/len(df)).sort_values().reset_index().convert_dtypes()



def get_df_meta(df):
    '''
    Вычисляет для каждого dataframe метаданные:

        percent_missing	- % пустых значений
                          (за пустые значения считаются и NaN и пустая строка)

        nunique - количество уникальных значений
                  (NaN не считается за значение, а пустая строка считается)

        datatypе - тип данных, к которому колонку автоматически преобразует pandas(numpy)
                   (помним что numpy трактует по умолчанию NaN как float,
                    а строковые колонки и колонки с разными типами данных как object )

    См. также: https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#integer-dtypes-and-missing-data

    '''

    percent_missing = ( df.isnull() | df.eq('') ).sum() * 100 / len(df)
    nunique = df.nunique()

    # diversity = dict()
    #
    # for col in df.columns:
    #     diversity[col] = len(df[col].unique())
    #
    # diversity_series =
    # value_counts for each row

    diversity_df = diversity_percentage(df, df.columns.to_list())
    diversity_df.rename(columns={ diversity_df.columns[0]: 'column_name', diversity_df.columns[1]: 'diversity' }, inplace = True)
    # diversity_df = diversity_df.rename(index={0: 'column_name', 1: 'diversity'}).reset_index()
    # diversity_df.info()
    # diversity_df.describe()
    # print(diversity_df)

    meta = pd.DataFrame({
        'column_name': df.columns,
        'percent_missing': percent_missing,
        'nunique': nunique,
        'datatype': df.dtypes
    })

    meta = pd.merge(meta, diversity_df, how='inner', on='column_name')
    return meta

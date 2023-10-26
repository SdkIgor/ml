#!/usr/bin/env python3
# Основной скрипт для управления нейросетью
import os
import sys
os.environ["PYCARET_CUSTOM_LOGGING_LEVEL"] = "CRITICAL"
os.environ["PYTHONPATH"]="$PWD/myguru-lib"  # export PYTHONPATH=$PWD/myguru-lib

'''
Что это? Основной скрипт для MLOps, обертка для модуля myguru_ml
Что делает? Выбирает лучшую модель исходя из входных данных
Когда запускать? При изменении модели данных

Примеры типичных вызовов:
    nn.py      (в cron по мере накоппления новых данныx - для улучшения нейросети)
    nn.py -v   (больше промежуточного вывода)
    nn.py -tvr (разметить, обучить, проверить)
    nn.py -sd  (просто проверяет всё ли ок с загрузкой модели)
    nn.py -st  (проверить работоспособность текущей модели, как юнит-тест)
    nn.py -e   (обработать папку с аудио)
'''

import argparse
import pandas as pd
from pycaret.classification import ClassificationExperiment

parser = argparse.ArgumentParser(description='Guru MLOps main script')


parser.add_argument('-e', '--emotionscore',
    action='store_true',
    help='Заново добавить в исходный csv файл данные скоринга эмоций')

parser.add_argument('-r', '--relabel',
    action='store_true',
    help='Заново обработать исходный csv методом guru_ml.preprocess_df')

parser.add_argument('-t', '--test',
    action='store_true',
    help='Протестировать созданную нейросеть')

parser.add_argument('-s', '--skiptraining',
    action='store_true',
    help='Пропускает этап тренеровки')

parser.add_argument('-d', '--devops',
    action='store_true',
    help='Создает Dockerfile, OpenAPI на FastAPI и Gradio')

parser.add_argument('-v', '--verbose',
    action='store_true',
    help='Больше промежуточного вывода')


parser.add_argument('-i', '--csvin',
    default='neuro/raw_with_emotions.csv',
    help='Файл с сырыми данными для обучения нейросети')

parser.add_argument('-o', '--csvout',
    default='neuro/fixed.csv',
    help='''
    Файл с обработанными данными для обучения нейросети.
    Учитывается если НЕ установлена опция -s (--skiptraining)
    ''')

parser.add_argument('-a', '--audiodatafolder',
    default='/home/a/projects/guru-audio/audio',
    help='Файл с сырыми данными для обучения нейросети')


args = parser.parse_args()


if args.emotionscore:
    from myguru_ml import add_emotional_scores
    df = pd.read_csv( args.csvin, sep=',', encoding='utf-8' )
    df = add_emotional_scores(df, audio_transcript_base_dir=args.audiodatafolder)
    # df.to_csv( args.csvin, sep=',', encoding='utf-8', index=False)
    df.to_csv( 'neuro/raw_with_emotions.csv', sep=',', encoding='utf-8', index=False)
    sys.exit()


if args.relabel:
    from myguru_ml import preprocess_df
    df = pd.read_csv( args.csvin, sep=',', encoding='utf-8' )
    df = preprocess_df(df, debug=args.verbose, save_csv=True, result_csv_filename=args.csvout)

s = ClassificationExperiment()

if not args.skiptraining:
    df = pd.read_csv( args.csvout, sep=',', encoding='utf-8' )
    s.setup(df, target = 'result')
    best_model = s.compare_models()
    s.save_model(best_model, 'neuro/my_best_pipeline')

loaded_model = s.load_model('neuro/my_best_pipeline')

if args.devops:
    s.create_api(loaded_model, 'ml_api')
    s.create_docker('ml_api')
    s.create_app(loaded_model)

if args.test:
    from myguru_ml import preprocess_df
    from myguru_crm import get_dsn

    # df = pd.read_sql('SELECT id user_id FROM user', get_dsn())
    df = pd.read_sql('SELECT id user_id FROM user WHERE status = 10', get_dsn())
    test_ids = df['user_id'].astype('int').tolist()

    test_df = pd.DataFrame({
        "user_id": { "0":test_ids[0], "1":test_ids[1],   "2":test_ids[2],  "3":test_ids[3] },
        "ab_group_id": { "0":1,    "1":2,   "2":3, "3":2 },
        "ab_question_id": { "0":1, "1":4, "2":1,  "3":7 },
    })

    # TODO: rand

    if args.verbose:
        print(test_df)

    test_df = preprocess_df(test_df, debug=False)

    if args.verbose:
        # test_df.to_csv('neuro/t.csv', sep=',', encoding='utf-8', index=False)
        test_df.info()
        print(test_df.head(2))

    predictions = s.predict_model(loaded_model, data=test_df, probability_threshold = 0.95)

    print(predictions.head()) # prediction_label, prediction_score

"""
Giskard Grafana Metrics Script
This script various drift metrics using the Giskard library and inserts these 
metrics into a PostgreSQL database for monitoring purposes.

It performs the following tasks:
1. Preparing the PostgreSQL database for storing metrics.
2. Calculating and extracting metrics using the Giskard library.
3. Inserting calculated metrics into the PostgreSQL database.

"""

import math
import time
import logging
import datetime

import pandas as pd
import psycopg
import pickle

from db import prep_db, insert_into_db
from giskard_drift_test_suites import test_drift_dataset_suite, dataset_drift_test
from giskard import Model, Dataset

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)

SEND_TIMEOUT = 10
begin = datetime.datetime(2023, 8, 1, 0, 0, tzinfo=datetime.timezone.utc)

CAT_COLS = ['holiday', 'weathersit']
NUM_COLS = ['temp', 'atemp', 'windspeed']
TARGET_COL = 'cnt'

# Load data and model
raw_data = pd.read_parquet('./data/raw_data.parquet')
reference_data = pd.read_parquet('./data/reference_data.parquet')

with open('./model/lr_model.pkl', 'rb') as f:
    lr = pickle.load(f)

def prediction_function(df):
    return lr.predict(df)

giskard_model = Model(
    model=prediction_function,
    model_type="regression"
)

wrapped_ref_dataset = Dataset(
    reference_data,
    cat_columns=CAT_COLS,
    target=TARGET_COL
)

def calculate_metrics_postgresql(cursor, i):
    """
    Calculate Metrics and Insert into PostgreSQL
    Calculate various metrics using the Giskard library and insert them into the PostgreSQL database.

    Args:
        curr: The PostgreSQL cursor.
        i (int): Index for processing the data in chunks.

    """
    current_data = raw_data.iloc[i * 100 : (i + 1) * 100]

    wrapped_curr_dataset = Dataset(
        current_data,
        cat_columns=CAT_COLS,
        target=TARGET_COL
    )
    
    # Test Suite for all Features and Target Variable
    features_target_drift_suite = test_drift_dataset_suite(
        suite_name='all_features_drift_test',
        cols=CAT_COLS + NUM_COLS,
        wrapped_ref_dataset = wrapped_ref_dataset,
        prediction_col_type = 'numeric',
        prediction_col = TARGET_COL
    )

    features_target_drift_suite_result = features_target_drift_suite.run(actual_dataset=wrapped_curr_dataset,
                                                                        reference_dataset=wrapped_ref_dataset,
                                                                        model=giskard_model)

    # Test Suite at Dataset Level
    dataset_drift_test_result = dataset_drift_test(features_target_drift_suite_result.results)

    # Test Suite for Important Features
    important_features = ['temp', 'atemp', 'windspeed']
    imp_features_drift_suite = test_drift_dataset_suite('important_features_drift_test',
                                                        important_features,
                                                        wrapped_ref_dataset)

    imp_features_drift_suite_results = imp_features_drift_suite.run(actual_dataset=wrapped_curr_dataset,
                                                                    reference_dataset=wrapped_ref_dataset)

    imp_features_dataset_drift_test = dataset_drift_test(imp_features_drift_suite_results.results, threshold=1)

    metrics = dict()

    for result in features_target_drift_suite_result.results[:-1]:
        metrics[result[2]['column_name'] + "_drift_score"] = round(result[1].metric, 5)
    metrics["target_drift_score"] = round(features_target_drift_suite_result.results[-1][1].metric, 5)

    metrics['dataset_drift_score'] = round(dataset_drift_test_result['metric'], 5)
    metrics['imp_var_drift_score'] = round(imp_features_dataset_drift_test['metric'], 5)

    insert_into_db(cursor, metrics, begin + datetime.timedelta(i))


def batch_monitoring():
    """
    Batch Monitoring Flow
    Prefect flow that orchestrates the monitoring process, including metric calculation and database insertion.

    """
    prep_db()
    iters = 5
    last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)
    
    with psycopg.connect(
        "host=localhost port=5432 dbname=giskard_monitoring user=postgres password=postgres",
        autocommit=True,
    ) as conn:
        for i in range(iters):
            with conn.cursor() as cursor:
                calculate_metrics_postgresql(cursor, i)

            new_send = datetime.datetime.now()
            seconds_elapsed = (new_send - last_send).total_seconds()
            if seconds_elapsed < SEND_TIMEOUT:
                time.sleep(SEND_TIMEOUT - seconds_elapsed)
            while last_send < new_send:
                last_send = last_send + datetime.timedelta(seconds=10)
            logging.info("data sent")

if __name__ == '__main__':
    batch_monitoring()

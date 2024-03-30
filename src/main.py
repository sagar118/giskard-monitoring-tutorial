"""
Giskard Grafana Data Drift Script.
This script calculates various drift tests using the Giskard library and inserts  
the results into the PostgreSQL database for storage.

It performs the following tasks:
1. Preparing the PostgreSQL database for storing results.
2. Calculating and extracting results using the Giskard library.
3. Inserting drift test results into the PostgreSQL database.

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

# Define the timestamp from which the monitoring process will begin
begin = datetime.datetime(2023, 12, 1, 0, 0, tzinfo=datetime.timezone.utc)

# Define the categorical and numerical columns
CAT_COLS = ['holiday', 'weathersit']
NUM_COLS = ['temp', 'atemp', 'windspeed']
TARGET_COL = 'cnt'

# Load data and model
raw_data = pd.read_parquet('./data/raw_data.parquet')
reference_data = pd.read_parquet('./data/reference_data.parquet')

# Load the model
with open('./model/lr_model.pkl', 'rb') as f:
    lr = pickle.load(f)

# Define the prediction function
def prediction_function(df):
    return lr.predict(df)

# Wrap the model into a Giskard Model object
giskard_model = Model(
    model=prediction_function,
    model_type="regression"
)

# Wrap the reference data into a Giskard Dataset object
wrapped_ref_dataset = Dataset(
    reference_data,
    cat_columns=CAT_COLS,
    target=TARGET_COL
)

def compute_drift_metrics(cursor, i):
    """
    Calculate Metrics and Insert into PostgreSQL
    Calculate various metrics using the Giskard library and insert them into the PostgreSQL database.

    Args:
        curr: The PostgreSQL cursor.
        i (int): Index for processing the data in chunks.

    """
    # Extract the current chunk of data
    # In a real-time monitoring scenario, this would be the new data
    current_data = raw_data.iloc[i * 100 : (i + 1) * 100]

    # Wrap the current chunk of data into a Giskard Dataset object
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

    # Run the test suite for all features and target variable
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
    # Run the test suite for important features
    imp_features_drift_suite_results = imp_features_drift_suite.run(actual_dataset=wrapped_curr_dataset,
                                                                    reference_dataset=wrapped_ref_dataset)
    
    # Run the dataset level test on the results of the important features test suite
    imp_features_dataset_drift_test = dataset_drift_test(imp_features_drift_suite_results.results, threshold=1)

    # Dictionary to store the metrics
    metrics = dict()

    # Extract the drift scores from the results of the test suites for input features
    for result in features_target_drift_suite_result.results[:-1]:
        metrics[result[2]['column_name'] + "_drift_score"] = round(result[1].metric, 5)

    # Extract the drift score from the results of the test suite for target variable
    metrics["target_drift_score"] = round(features_target_drift_suite_result.results[-1][1].metric, 5)

    # Extract the drift scores from the results of the test suites for dataset level and important features
    metrics['dataset_drift_score'] = round(dataset_drift_test_result['metric'], 5)
    metrics['imp_var_drift_score'] = round(imp_features_dataset_drift_test['metric'], 5)

    # Insert the metrics into the PostgreSQL database
    # We increase the timestamp by 1 day for each iteration to simulate a real-time monitoring scenario
    insert_into_db(cursor, metrics, begin + datetime.timedelta(i))


def batch_monitoring():
    """
    Batch Monitoring Flow
    Prefect flow that orchestrates the monitoring process, including metric calculation and database insertion.

    """
    # Prepare the PostgreSQL database
    prep_db()
    
    # Number of iterations to run the monitoring process
    iters = 5
    
    # Connect to the PostgreSQL database and calculate drift metrics for 5 chunks of data
    with psycopg.connect(
        "host=localhost port=5432 dbname=giskard_monitoring user=postgres password=postgres",
        autocommit=True,
    ) as conn:
        for i in range(iters):
            with conn.cursor() as cursor:
                compute_drift_metrics(cursor, i) # Calculate drift metrics and insert into PostgreSQL

if __name__ == '__main__':
    batch_monitoring()

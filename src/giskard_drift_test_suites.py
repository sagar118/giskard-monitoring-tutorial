from giskard import testing, Suite, Dataset

def _check_test(col_type: str, 
                column_name: str, 
                num_rows: int, 
                prediction_flag: bool = False, 
                NUM_ROWS_THRESHOLD: int = 1000):
    
    is_small_dataset = num_rows <= NUM_ROWS_THRESHOLD
    drift_test_function = None

    if col_type == 'numeric':
        if is_small_dataset:
            drift_test_function = testing.test_drift_prediction_ks if prediction_flag else testing.test_drift_ks
        else:
            drift_test_function = testing.test_drift_prediction_earth_movers_distance if prediction_flag else testing.test_drift_earth_movers_distance

    elif col_type == 'categorical':
        if is_small_dataset:
            drift_test_function = testing.test_drift_prediction_psi if prediction_flag else testing.test_drift_psi
        else:
            drift_test_function = testing.test_drift_prediction_chi_square if prediction_flag else testing.test_drift_chi_square

    if drift_test_function:
        return drift_test_function(column_name=column_name)

    raise ValueError(f"Unsupported column type: {col_type}")

def test_drift_dataset_suite(suite_name: str,
                             cols: list,
                             wrapped_ref_dataset: Dataset,
                             prediction_col_type: str = None,
                             prediction_col: str = None):
    N = len(wrapped_ref_dataset.df)

    suite = Suite(name=suite_name)

    for col in cols:
        if col in wrapped_ref_dataset.category_features.keys():
            test = _check_test('categorical', col, N)
        else:
            test = _check_test('numeric', col, N)
        
        suite.add_test(test)

    if prediction_col is not None:
        test = _check_test(prediction_col_type, prediction_col, N, True)
        suite.add_test(test)

    return suite

def dataset_drift_test(test_suite_results: list,
                       threshold: float = 0.5):

    raw_results = test_suite_results
    count, N = 0, len(raw_results)

    for result in raw_results:
        if 'succeed' in str(result[1]): count += 1
    passed = (count / N) > threshold

    return {
        'passed': passed,
        'metric': (count / N)
    }
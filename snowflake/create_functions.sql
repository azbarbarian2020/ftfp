-- ============================================================================
-- NEW_FTFP Function Creation
-- Created: 2026-04-04
-- ============================================================================
--
-- ML UDFs pointing to @NEW_FTFP.DATA.ML_MODELS stage.
-- GET_OPTIMAL_SERVICE is REMOVED -- routing handled by Python + Valhalla.
-- ============================================================================

USE SCHEMA NEW_FTFP.DATA;

-- ===========================================================================
-- Function 1: CLASSIFY_FAILURE_ML (11 features -> failure type)
-- ===========================================================================

CREATE OR REPLACE FUNCTION CLASSIFY_FAILURE_ML(
    AVG_ENGINE_TEMP FLOAT,
    AVG_TRANS_OIL_PRESSURE FLOAT,
    AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT,
    STDDEV_ENGINE_TEMP FLOAT,
    STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT,
    SLOPE_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT,
    ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'classify'
IMPORTS = (
    '@NEW_FTFP.DATA.ML_MODELS/models/classifier_v1_0_0.pkl.gz',
    '@NEW_FTFP.DATA.ML_MODELS/models/label_mapping_v1_0_0.pkl.gz',
    '@NEW_FTFP.DATA.ML_MODELS/models/feature_columns_v1_0_0.pkl.gz'
)
COMMENT='ML-based failure classification'
AS $$
import sys
import joblib
import numpy as np

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

clf_model = joblib.load(import_dir + "classifier_v1_0_0.pkl.gz")
label_info = joblib.load(import_dir + "label_mapping_v1_0_0.pkl.gz")
reverse_label_mapping = label_info["reverse_mapping"]

def classify(avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage,
             stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure,
             slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage,
             rolling_avg_engine_temp, rolling_avg_trans_oil_pressure):
    features = np.array([[
        avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage,
        stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure,
        slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage,
        rolling_avg_engine_temp, rolling_avg_trans_oil_pressure
    ]])
    if np.isnan(features).any():
        return "NORMAL"
    prediction = clf_model.predict(features)[0]
    return reverse_label_mapping[int(prediction)]
$$;

-- ===========================================================================
-- Function 2: PREDICT_TTF_ML (11 features -> hours to failure)
-- ===========================================================================

CREATE OR REPLACE FUNCTION PREDICT_TTF_ML(
    AVG_ENGINE_TEMP FLOAT,
    AVG_TRANS_OIL_PRESSURE FLOAT,
    AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT,
    STDDEV_ENGINE_TEMP FLOAT,
    STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT,
    SLOPE_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT,
    ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'predict_ttf'
IMPORTS = ('@NEW_FTFP.DATA.ML_MODELS/models/regression_v1_0_0.pkl.gz')
COMMENT='TTF prediction - basic_11_features'
AS $$
import sys
import joblib
import numpy as np

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
reg_model = joblib.load(import_dir + "regression_v1_0_0.pkl.gz")

def predict_ttf(avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage,
                stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure,
                slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage,
                rolling_avg_engine_temp, rolling_avg_trans_oil_pressure):
    features = np.array([[
        avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage,
        stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure,
        slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage,
        rolling_avg_engine_temp, rolling_avg_trans_oil_pressure
    ]])
    if np.isnan(features).any():
        return 0.0
    prediction = reg_model.predict(features)[0]
    return max(0.1, min(24.0, float(prediction)))
$$;

-- ===========================================================================
-- Function 3: PREDICT_TTF_TEMPORAL (16 features -> hours to failure)
-- ===========================================================================

CREATE OR REPLACE FUNCTION PREDICT_TTF_TEMPORAL(
    AVG_ENGINE_TEMP FLOAT,
    AVG_TRANS_OIL_PRESSURE FLOAT,
    AVG_BATTERY_VOLTAGE FLOAT,
    STDDEV_BATTERY_VOLTAGE FLOAT,
    STDDEV_ENGINE_TEMP FLOAT,
    STDDEV_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_ENGINE_TEMP FLOAT,
    SLOPE_TRANS_OIL_PRESSURE FLOAT,
    SLOPE_BATTERY_VOLTAGE FLOAT,
    ROLLING_AVG_ENGINE_TEMP FLOAT,
    ROLLING_AVG_TRANS_OIL_PRESSURE FLOAT,
    CUMULATIVE_VOLATILITY FLOAT,
    ELEVATED_WINDOW_COUNT FLOAT,
    VOLATILITY_DELTA FLOAT,
    TEMP_ACCELERATION FLOAT,
    PRESSURE_ACCELERATION FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('xgboost','numpy','pandas','scikit-learn','joblib')
HANDLER = 'predict_ttf_temporal'
IMPORTS = ('@NEW_FTFP.DATA.ML_MODELS/models/regression_temporal_v1_1_0.pkl.gz')
COMMENT='TTF prediction - temporal_16_features'
AS $$
import sys
import joblib
import numpy as np

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
reg_temporal_model = joblib.load(import_dir + "regression_temporal_v1_1_0.pkl.gz")

def predict_ttf_temporal(avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage,
                        stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure,
                        slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage,
                        rolling_avg_engine_temp, rolling_avg_trans_oil_pressure,
                        cumulative_volatility, elevated_window_count, volatility_delta,
                        temp_acceleration, pressure_acceleration):
    features = np.array([[
        avg_engine_temp, avg_trans_oil_pressure, avg_battery_voltage,
        stddev_battery_voltage, stddev_engine_temp, stddev_trans_oil_pressure,
        slope_engine_temp, slope_trans_oil_pressure, slope_battery_voltage,
        rolling_avg_engine_temp, rolling_avg_trans_oil_pressure,
        cumulative_volatility, elevated_window_count, volatility_delta,
        temp_acceleration, pressure_acceleration
    ]])
    if np.isnan(features).any():
        return 0.0
    prediction = reg_temporal_model.predict(features)[0]
    return max(0.1, min(24.0, float(prediction)))
$$;

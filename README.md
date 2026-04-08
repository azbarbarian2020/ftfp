# Fleet Telemetry Failure Prediction (FTFP) v5.4.4

Real-time fleet monitoring demo running on Snowpark Container Services (SPCS). Simulates 10 trucks with live telemetry, ML-powered failure prediction, and interactive map visualization.

## What This Demo Shows

- **Real-time telemetry simulation** -- 10 trucks streaming engine temp, transmission oil pressure, and battery voltage every 5 seconds
- **ML failure prediction** -- 3 XGBoost models classify failure type and predict time-to-failure in real-time
- **Interactive map** -- Leaflet-based map showing truck positions, routes, service centers, and detour routing
- **Failure injection** -- Inject engine, transmission, or electrical failures on any truck and watch ML predictions respond

## Architecture

```
                    Browser (React + Leaflet)
                              |
                              v
                 SPCS Service (FastAPI/Uvicorn)
                    /         |          \
                   /          |           \
          Read Pool(3)   Write Conn    Snowflake SQL
              |               |              |
    PREDICTION_CACHE    TELEMETRY     ML UDFs + Views
    TRUCK_ROUTES        STREAM_STATE  FEATURE_ENGINEERING
    SERVICE_CENTERS     ACTIVE_FAILURES
    ROUTE_WAYPOINTS     FAILURE_CONFIG
```

### FastAPI Backend (`main.py`, ~2000 lines)

- Dual Snowflake connections: read pool (3 connections) for queries, dedicated write connection for telemetry inserts
- Background telemetry writer reads from seed tables, injects failure patterns, writes to TELEMETRY table
- Prediction engine queries ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF and caches results
- REST endpoints serve map data, truck positions, predictions, failure controls

### Frontend (React + Leaflet)

- Compiled React app with custom JS overlay in `index.html`
- Interactive map with truck markers, route polylines, service center icons
- Sidebar controls for failure injection, time controls, truck selection
- Real-time prediction display with color-coded severity

## ML Pipeline

### Training Data

`TRAINING_TBL` contains 2,931,379 rows of labeled telemetry from 40 simulated trucks. Each row has:

| Column | Type | Description |
|--------|------|-------------|
| TIMESTAMP | TIMESTAMP_NTZ | Observation time |
| ENTITY_ID | VARCHAR | Truck identifier (TRUCK-001 to TRUCK-040) |
| FAILURE_TYPE | VARCHAR | NORMAL, ENGINE_FAILURE, TRANSMISSION_FAILURE, ELECTRICAL_FAILURE |
| TIME_TO_FAILURE | FLOAT | Hours until failure occurs (0 at failure point) |
| ENGINE_TEMP | FLOAT | Engine temperature (F) |
| TRANS_OIL_PRESSURE | FLOAT | Transmission oil pressure (PSI) |
| BATTERY_VOLTAGE | FLOAT | Battery voltage (V) |
| FAILURE_FLAG | NUMBER | 1 = in failure window, 0 = normal |

### Feature Engineering

The `FEATURE_ENGINEERING_VIEW_TEMPORAL` view transforms raw telemetry into ML features using 5-minute sliding windows:

**Basic features (11):**
- AVG_ENGINE_TEMP, AVG_TRANS_OIL_PRESSURE, AVG_BATTERY_VOLTAGE
- STDDEV_ENGINE_TEMP, STDDEV_TRANS_OIL_PRESSURE, STDDEV_BATTERY_VOLTAGE
- SLOPE_ENGINE_TEMP, SLOPE_TRANS_OIL_PRESSURE, SLOPE_BATTERY_VOLTAGE
- ROLLING_AVG_ENGINE_TEMP, ROLLING_AVG_TRANS_OIL_PRESSURE

**Temporal features (5 additional, for electrical failures):**
- CUMULATIVE_VOLATILITY -- total accumulated battery instability over time
- ELEVATED_WINDOW_COUNT -- number of windows with high voltage stddev (>0.7V)
- VOLATILITY_DELTA -- change in volatility between consecutive windows
- TEMP_ACCELERATION -- second derivative of temperature change
- PRESSURE_ACCELERATION -- second derivative of pressure change

### Three ML Models

| Model | File | Input | Output | Used For |
|-------|------|-------|--------|----------|
| Classifier | classifier_v1_0_0.pkl.gz | 11 features | Failure type (VARCHAR) | Determine failure category |
| TTF Basic | regression_v1_0_0.pkl.gz | 11 features | Hours to failure (FLOAT) | Engine & transmission TTF |
| TTF Temporal | regression_temporal_v1_1_0.pkl.gz | 16 features | Hours to failure (FLOAT) | Electrical TTF |

Supporting artifacts: `label_mapping_v1_0_0.pkl.gz`, `feature_columns_v1_0_0.pkl.gz`, `feature_columns_temporal_v1_1_0.pkl.gz`

### Hybrid Prediction Flow

```
Raw Telemetry
    |
    v
FEATURE_ENGINEERING_VIEW_TEMPORAL (5-min aggregations)
    |
    v
CLASSIFY_FAILURE_ML (11 features -> failure type)
    |
    +--> ENGINE/TRANSMISSION --> PREDICT_TTF_ML (11 features -> hours)
    |
    +--> ELECTRICAL ----------> PREDICT_TTF_TEMPORAL (16 features -> hours)
    |
    +--> NORMAL --------------> NULL (no failure predicted)
    |
    v
ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF (final output)
    |
    v
PREDICTION_CACHE (API reads cached results)
```

### Three UDFs in NEW_FTFP.DATA

1. **CLASSIFY_FAILURE_ML**(11 FLOAT) -> VARCHAR -- XGBoost classifier, returns failure type
2. **PREDICT_TTF_ML**(11 FLOAT) -> FLOAT -- XGBoost regressor for engine/transmission TTF
3. **PREDICT_TTF_TEMPORAL**(16 FLOAT) -> FLOAT -- XGBoost regressor for electrical TTF (uses temporal features)

All UDFs load models from `@ML_MODELS/models/` using `joblib` + `xgboost` packages.

## Seed Data Tables

| Table | Rows | Size | Description |
|-------|------|------|-------------|
| NORMAL_SEED | 1,209,610 | 26MB | Normal operating telemetry patterns |
| ENGINE_FAILURE_SEED | 7,920 | 162KB | Engine failure degradation patterns |
| TRANSMISSION_FAILURE_SEED | 8,640 | 177KB | Transmission failure patterns |
| ELECTRICAL_FAILURE_SEED | 11,520 | 238KB | Electrical failure patterns |
| TRUCK_ROUTES | 10 | 2KB | Route definitions (10 routes across US) |
| ROUTE_WAYPOINTS | ~50K | compressed | GPS waypoints for each route |
| SERVICE_CENTERS | ~50 | 5KB | Service center locations and details |
| SERVICE_CENTER_ROUTES | ~500 | 10KB | Pre-computed detour metrics |
| FREIGHT_NODES | ~200 | 5KB | Highway network nodes |
| FREIGHT_EDGES | ~1000 | compressed | Highway network edges with geometry |
| TRUCK_ROUTE_NODES | ~100 | 2KB | Truck-to-route-node mapping |
| DETOUR_ROUTES | 1,081 | 122MB | Pre-computed detour route geometries |
| FAILURE_CONFIG | 10 | 1KB | Per-truck failure injection config |
| FIRST_FAILURE_MARKERS | 10 | 1KB | First failure detection timestamps |
| TRAINING_TBL | 2,931,379 | ~69MB | Labeled telemetry for ML training |

## Prerequisites

- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (`snow`) with a configured connection
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- Python 3.10+
- OpenSSL

## Quick Start

```bash
git clone <repo-url>
cd ftfp
./setup.sh
```

The setup script will:
1. Prompt for your Snowflake CLI connection name
2. Auto-detect account, registry, and user
3. Create database, schema, warehouse, tables, stages
4. Load all seed data (CSVs), training data, and ML model artifacts
5. Create views and ML UDFs
6. Set up RSA key-pair authentication (safely handles existing keys)
7. Build and push Docker image to Snowflake image registry
8. Deploy SPCS service and wait for it to become READY
9. Print the application URL

Total setup time: ~10-15 minutes (mostly Docker build + push and service startup).

## Teardown

```bash
./teardown.sh
```

Drops the service, compute pool, database, and warehouse. Preserves RSA keys.

## Notebooks

Two Snowflake notebooks are included in `notebooks/`:

### ML_PIPELINE_BUILD_DEPLOY.ipynb

The **primary notebook** that creates the entire ML system from scratch:
- Trains 3 XGBoost models on TRAINING_TBL data
- Saves 6 model artifacts to @ML_MODELS stage
- Deploys 3 Python UDFs (CLASSIFY_FAILURE_ML, PREDICT_TTF_ML, PREDICT_TTF_TEMPORAL)
- Creates the feature engineering and prediction views

Use this notebook to **retrain or rebuild** the ML models.

### ML_PIPELINE_DOCUMENTATION.ipynb

A **read-only companion** that documents and validates the ML infrastructure:
- Explains every feature, model, and UDF with rich markdown
- Contains architecture diagrams and performance characteristics
- Uses GET_DDL() to inspect deployed objects
- Tests prediction pipeline with sample queries
- Verifies seed data integrity

Use this notebook for **understanding and demoing** the ML pipeline.

## File Structure

```
ftfp/
  setup.sh                    # Automated setup (7 steps)
  teardown.sh                 # Clean teardown
  Dockerfile                  # Python 3.10 + FastAPI
  ftfp_service.yaml.template  # SPCS spec (image path templated)
  .gitignore
  README.md
  backend/
    main.py                   # FastAPI app (~2000 lines, v5.4.4)
    requirements.txt          # Python dependencies
    frontend/
      build/                  # Compiled React app + custom overlay JS
        index.html
        static/
  snowflake/
    setup.sql                 # All DDL: database, tables, stages
    create_views.sql          # 4 views
    create_functions.sql      # 3 ML UDFs
  data/
    normal_seed.csv.gz        # Seed data files (14 files)
    engine_failure_seed.csv.gz
    transmission_failure_seed.csv.gz
    electrical_failure_seed.csv.gz
    truck_routes.csv
    route_waypoints.csv.gz
    service_centers.csv
    service_center_routes.csv
    freight_nodes.csv
    freight_edges.csv.gz
    truck_route_nodes.csv
    detour_routes.csv.gz
    failure_config.csv
    first_failure_markers.csv
    training/                 # ML training data (16 gzipped CSVs, ~69MB)
      training_tbl_*.csv.gz
  models/                     # Pre-trained ML model artifacts (6 files)
    classifier_v1_0_0.pkl.gz
    label_mapping_v1_0_0.pkl.gz
    feature_columns_v1_0_0.pkl.gz
    regression_v1_0_0.pkl.gz
    regression_temporal_v1_1_0.pkl.gz
    feature_columns_temporal_v1_1_0.pkl.gz
  notebooks/
    ML_PIPELINE_BUILD_DEPLOY.ipynb    # Trains + deploys ML models
    ML_PIPELINE_DOCUMENTATION.ipynb   # Documents + validates ML system
```

## Snowflake Objects Created

| Object | Type | Purpose |
|--------|------|---------|
| NEW_FTFP | Database | All FTFP objects |
| NEW_FTFP.DATA | Schema | Single schema for everything |
| NEW_FTFP_WH | Warehouse | XS, auto-suspend 60s |
| NEW_FTFP_POOL | Compute Pool | CPU_X64_S, 1 node |
| FTFP_SERVICE | SPCS Service | FastAPI container |
| FTFP_REPO | Image Repository | Docker images |
| ML_MODELS | Stage | Model .pkl.gz files |
| DATA_STAGE | Stage | CSV data loading |
| TELEMETRY_5MIN_AGG | View | 5-minute telemetry aggregations |
| FEATURE_ENGINEERING_VIEW_TEMPORAL | View | ML feature engineering (16 features) |
| ROUTE_GEOMETRY_DETAILED | View | Route visualization geometry |
| ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF | View | Final prediction output |
| CLASSIFY_FAILURE_ML | UDF | Failure type classifier |
| PREDICT_TTF_ML | UDF | TTF for engine/transmission |
| PREDICT_TTF_TEMPORAL | UDF | TTF for electrical |

## Resetting the Simulation

To clear runtime state and restart the simulation from scratch:

```sql
TRUNCATE TABLE NEW_FTFP.DATA.TELEMETRY;
TRUNCATE TABLE NEW_FTFP.DATA.PREDICTION_CACHE;
TRUNCATE TABLE NEW_FTFP.DATA.ACTIVE_FAILURES;
TRUNCATE TABLE NEW_FTFP.DATA.FAILURE_CONFIG;
TRUNCATE TABLE NEW_FTFP.DATA.FIRST_FAILURE_MARKERS;
UPDATE NEW_FTFP.DATA.STREAM_STATE
  SET NEXT_EPOCH = 0, LAST_UPDATED = CURRENT_TIMESTAMP()
  WHERE STREAM_NAME = 'NORMAL_TO_TELEMETRY';
```

Then restart the SPCS service:
```sql
ALTER SERVICE NEW_FTFP.DATA.FTFP_SERVICE SUSPEND;
ALTER SERVICE NEW_FTFP.DATA.FTFP_SERVICE RESUME;
```

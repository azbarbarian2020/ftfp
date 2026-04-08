from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import asyncio
import json
import logging
import sys
from typing import List, Dict, Optional
import snowflake.connector
import threading
import time
import os
from pathlib import Path
from collections import OrderedDict
import heapq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="FTFP API", version="5.4.4")
APP_VERSION = "v5.4.4"

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

@app.middleware("http")
async def no_cache_middleware(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

SPEED_MPH = 55.0
SECONDS_PER_EPOCH = 5
MILES_PER_EPOCH = SPEED_MPH * SECONDS_PER_EPOCH / 3600
SAFETY_FACTOR = 0.85
TELEMETRY_RETENTION_HOURS = 48
_last_prune_time = 0

refresh_lock = threading.Lock()
refresh_in_progress = False
last_refresh_time = 0
pending_refresh_thread = None

cache_lock = threading.Lock()
telemetry_cache = {"data": None, "timestamp": 0}
predictions_cache = {"data": None, "timestamp": 0}
failures_cache = {"data": None, "timestamp": 0}
chart_cache = {"data": None, "timestamp": 0, "hours": None}
markers_cache = {"data": None, "timestamp": 0}
map_trucks_cache = {"data": None, "timestamp": 0, "epoch": 0}
map_routes_cache = {"data": None, "timestamp": 0}
map_services_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 2
MAP_CACHE_TTL = 3
PREDICTIONS_CACHE_TTL = 3
STATIC_CACHE_TTL = 300
FAILURES_CACHE_TTL = 5
epoch_cache = {"epoch": 0, "timestamp": 0}
EPOCH_CACHE_TTL = 2

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

session = None
conn = None
read_conn = None
read_conn_pool = []
READ_POOL_SIZE = 3
read_pool_lock = threading.Lock()
read_pool_idx = 0
write_conn = None
write_lock = threading.Lock()
_write_conn_is_spcs = None
_write_conn_token = None
SCHEMA_PREFIX = None
STREAM_NAME = "NORMAL_TO_TELEMETRY"

TELEMETRY = "NEW_FTFP.DATA.TELEMETRY"
NORMAL_SEED = "NEW_FTFP.DATA.NORMAL_SEED"
ENGINE_FAILURE_SEED = "NEW_FTFP.DATA.ENGINE_FAILURE_SEED"
TRANSMISSION_FAILURE_SEED = "NEW_FTFP.DATA.TRANSMISSION_FAILURE_SEED"
ELECTRICAL_FAILURE_SEED = "NEW_FTFP.DATA.ELECTRICAL_FAILURE_SEED"
FAILURE_CONFIG = "NEW_FTFP.DATA.FAILURE_CONFIG"
STREAM_STATE = "NEW_FTFP.DATA.STREAM_STATE"
PREDICTION_CACHE = "NEW_FTFP.DATA.PREDICTION_CACHE"
ACTIVE_FAILURES = "NEW_FTFP.DATA.ACTIVE_FAILURES"
FIRST_FAILURE_MARKERS = "NEW_FTFP.DATA.FIRST_FAILURE_MARKERS"
TELEMETRY_5MIN_AGG = "NEW_FTFP.DATA.TELEMETRY_5MIN_AGG"
ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF = "NEW_FTFP.DATA.ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF"


def get_schema_prefix():
    db_name = os.getenv('SNOWFLAKE_DATABASE')
    schema_name = os.getenv('SNOWFLAKE_SCHEMA')
    if db_name and schema_name:
        prefix = f"{db_name}.{schema_name}"
        logger.info(f"Using DB/SCHEMA env vars: {prefix}")
        return prefix
    logger.warning("No DB/SCHEMA env vars - using defaults")
    return "NEW_FTFP.DATA"


def update_table_constants(schema_prefix):
    global TELEMETRY, NORMAL_SEED, ENGINE_FAILURE_SEED, TRANSMISSION_FAILURE_SEED
    global ELECTRICAL_FAILURE_SEED, FAILURE_CONFIG, STREAM_STATE
    global PREDICTION_CACHE, ACTIVE_FAILURES, FIRST_FAILURE_MARKERS
    global TELEMETRY_5MIN_AGG, ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF

    TELEMETRY = f"{schema_prefix}.TELEMETRY"
    NORMAL_SEED = f"{schema_prefix}.NORMAL_SEED"
    ENGINE_FAILURE_SEED = f"{schema_prefix}.ENGINE_FAILURE_SEED"
    TRANSMISSION_FAILURE_SEED = f"{schema_prefix}.TRANSMISSION_FAILURE_SEED"
    ELECTRICAL_FAILURE_SEED = f"{schema_prefix}.ELECTRICAL_FAILURE_SEED"
    FAILURE_CONFIG = f"{schema_prefix}.FAILURE_CONFIG"
    STREAM_STATE = f"{schema_prefix}.STREAM_STATE"
    PREDICTION_CACHE = f"{schema_prefix}.PREDICTION_CACHE"
    ACTIVE_FAILURES = f"{schema_prefix}.ACTIVE_FAILURES"
    FIRST_FAILURE_MARKERS = f"{schema_prefix}.FIRST_FAILURE_MARKERS"
    TELEMETRY_5MIN_AGG = f"{schema_prefix}.TELEMETRY_5MIN_AGG"
    ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF = f"{schema_prefix}.ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF"
    logger.info(f"Table constants updated: {TELEMETRY}")


def _create_write_connection(is_spcs, token=None):
    if is_spcs:
        return snowflake.connector.connect(
            host=os.getenv('SNOWFLAKE_HOST'),
            port=os.getenv('SNOWFLAKE_PORT'),
            protocol="https",
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            authenticator="oauth",
            token=token,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            client_session_keep_alive=True,
            autocommit=True,
            insecure_mode=True
        )
    else:
        snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
        if snowflake_password:
            return snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT", "SFSENORTHAMERICA-AWSBARBARIAN"),
                user=os.getenv("SNOWFLAKE_USER", "Horizonadmin"),
                password=snowflake_password,
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "NEW_FTFP_WH"),
                database=os.getenv("SNOWFLAKE_DATABASE", "NEW_FTFP"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "DATA"),
                client_session_keep_alive=True,
                autocommit=True
            )
        else:
            conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "awsbarbarian_CoCo")
            return snowflake.connector.connect(connection_name=conn_name)


def _reconnect_write_conn():
    global write_conn
    try:
        if write_conn:
            try:
                write_conn.close()
            except Exception:
                pass
        if _write_conn_is_spcs:
            token_file = "/snowflake/session/token"
            with open(token_file, 'r') as f:
                fresh_token = f.read().strip()
            write_conn = _create_write_connection(True, fresh_token)
        else:
            write_conn = _create_write_connection(False)
        logger.info("Write connection reconnected")
    except Exception as e:
        logger.error(f"Write reconnect failed: {e}")
        write_conn = None

def _is_conn_alive(c):
    try:
        c.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False

def execute_write_sql(query, num_statements=1):
    global write_conn
    if not write_conn:
        _reconnect_write_conn()
    if not write_conn:
        logger.error("No write connection available")
        return
    try:
        cur = write_conn.cursor()
        cur.execute(query, num_statements=num_statements)
        cur.close()
    except Exception as e:
        logger.error(f"Write SQL error: {e}, attempting reconnect")
        _reconnect_write_conn()
        if write_conn:
            try:
                cur = write_conn.cursor()
                cur.execute(query, num_statements=num_statements)
                cur.close()
            except Exception as e2:
                logger.error(f"Write SQL retry failed: {e2}")


def execute_write_query(query):
    global write_conn
    if not write_conn:
        _reconnect_write_conn()
    if not write_conn:
        logger.error("No write connection available")
        return pd.DataFrame()
    try:
        cur = write_conn.cursor()
        cur.execute(query)
        cols = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        logger.error(f"Write query error: {e}, attempting reconnect")
        _reconnect_write_conn()
        if write_conn:
            try:
                cur = write_conn.cursor()
                cur.execute(query)
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                cur.close()
                if not rows:
                    return pd.DataFrame(columns=cols)
                return pd.DataFrame(rows, columns=cols)
            except Exception as e2:
                logger.error(f"Write query retry failed: {e2}")
        return pd.DataFrame()


@app.on_event("startup")
async def startup_event():
    global session, conn, read_conn, read_conn_pool, write_conn, SCHEMA_PREFIX, _write_conn_is_spcs, _write_conn_token
    logger.info("=" * 60)
    logger.info("STARTING SNOWFLAKE CONNECTION")
    logger.info("=" * 60)

    SCHEMA_PREFIX = get_schema_prefix()
    update_table_constants(SCHEMA_PREFIX)

    token_file = "/snowflake/session/token"
    is_spcs = os.path.exists(token_file)

    if is_spcs:
        logger.info("Running in SPCS - using OAuth token")
        try:
            with open(token_file, 'r') as f:
                token = f.read().strip()
            conn = snowflake.connector.connect(
                host=os.getenv('SNOWFLAKE_HOST'),
                port=os.getenv('SNOWFLAKE_PORT'),
                protocol="https",
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                authenticator="oauth",
                token=token,
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
                client_session_keep_alive=True,
                autocommit=True,
                insecure_mode=True
            )
            from snowflake.snowpark import Session
            session = Session.builder.configs({"connection": conn}).create()
            read_conn_pool = []
            for i in range(READ_POOL_SIZE):
                rc = snowflake.connector.connect(
                    host=os.getenv('SNOWFLAKE_HOST'),
                    port=os.getenv('SNOWFLAKE_PORT'),
                    protocol="https",
                    account=os.getenv('SNOWFLAKE_ACCOUNT'),
                    authenticator="oauth",
                    token=token,
                    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                    database=os.getenv('SNOWFLAKE_DATABASE'),
                    schema=os.getenv('SNOWFLAKE_SCHEMA'),
                    client_session_keep_alive=True,
                    autocommit=True,
                    insecure_mode=True
                )
                read_conn_pool.append(rc)
            read_conn = read_conn_pool[0]
            _write_conn_is_spcs = True
            _write_conn_token = token
            write_conn = _create_write_connection(True, token)
            logger.info(f"SPCS CONNECTION SUCCESSFUL ({READ_POOL_SIZE} read + 1 write)")
            load_freight_graph(SCHEMA_PREFIX or "NEW_FTFP.DATA")
            preload_routing_data(SCHEMA_PREFIX or "NEW_FTFP.DATA")
        except Exception as e:
            logger.error(f"SPCS CONNECTION FAILED: {e}")
            import traceback
            traceback.print_exc()
            session = None
            conn = None
            read_conn = None
            read_conn_pool = []
            write_conn = None
    else:
        logger.info("Running locally")
        try:
            from snowflake.snowpark import Session
            snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
            if snowflake_password:
                session = Session.builder.configs({
                    "account": os.getenv("SNOWFLAKE_ACCOUNT", "SFSENORTHAMERICA-AWSBARBARIAN"),
                    "user": os.getenv("SNOWFLAKE_USER", "Horizonadmin"),
                    "password": snowflake_password,
                    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "NEW_FTFP_WH"),
                    "database": os.getenv("SNOWFLAKE_DATABASE", "NEW_FTFP"),
                    "schema": os.getenv("SNOWFLAKE_SCHEMA", "DATA"),
                }).create()
            else:
                conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "awsbarbarian_CoCo")
                session = Session.builder.config("connection_name", conn_name).create()
            conn = session._conn._conn
            read_conn_pool = []
            for i in range(READ_POOL_SIZE):
                read_conn_pool.append(_create_write_connection(False))
            read_conn = read_conn_pool[0]
            _write_conn_is_spcs = False
            _write_conn_token = None
            write_conn = _create_write_connection(False)
            logger.info(f"Local connection established ({READ_POOL_SIZE} read + 1 write)")
            load_freight_graph(SCHEMA_PREFIX or "NEW_FTFP.DATA")
            preload_routing_data(SCHEMA_PREFIX or "NEW_FTFP.DATA")
        except Exception as e:
            logger.error(f"Local connection failed: {e}")
            session = None
            conn = None
            read_conn = None
            read_conn_pool = []
            write_conn = None


@app.on_event("shutdown")
async def shutdown_event():
    global conn, read_conn, write_conn, read_conn_pool
    for c in read_conn_pool:
        try:
            c.close()
        except:
            pass
    for c in [conn, write_conn]:
        if c:
            try:
                c.close()
            except:
                pass


def _get_read_conn():
    global read_pool_idx
    if read_conn_pool:
        with read_pool_lock:
            c = read_conn_pool[read_pool_idx % len(read_conn_pool)]
            read_pool_idx += 1
            return c
    return read_conn

def execute_query(query):
    c = _get_read_conn()
    if not c:
        return pd.DataFrame()
    try:
        cur = c.cursor()
        cur.execute(query)
        cols = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        logger.error(f"Read query error: {e}")
        return pd.DataFrame()


def execute_sql(query):
    c = _get_read_conn()
    if not c:
        return
    try:
        cur = c.cursor()
        cur.execute(query)
        cur.close()
    except Exception as e:
        logger.error(f"SQL error: {e}")


def maybe_prune_telemetry():
    global _last_prune_time
    now = time.time()
    if now - _last_prune_time < 60:
        return
    _last_prune_time = now
    try:
        execute_write_sql(f"""
            DELETE FROM {TELEMETRY}
            WHERE TIMESTAMP < DATEADD('hour', -{TELEMETRY_RETENTION_HOURS},
                (SELECT MAX(TIMESTAMP) FROM {TELEMETRY}))
        """)
    except Exception as e:
        logger.warning(f"Telemetry pruning failed: {e}")


@app.get("/api/health")
async def health():
    return {"message": f"FTFP API {APP_VERSION}", "status": "running", "speed_mph": SPEED_MPH, "version": APP_VERSION}


@app.post("/api/initialize")
def initialize_database():
    try:
        acquired = write_lock.acquire(timeout=30)
        if not acquired:
            return {"status": "error", "message": "Write lock timeout"}
        try:
            execute_write_sql(f"""
                CREATE TABLE IF NOT EXISTS {STREAM_STATE} (
                    stream_name STRING PRIMARY KEY,
                    start_ts TIMESTAMP_NTZ,
                    step_seconds NUMBER(38,0),
                    next_epoch NUMBER(38,0)
                )
            """)
            execute_write_sql(f"""
                CREATE TABLE IF NOT EXISTS {FAILURE_CONFIG} (
                    entity_id STRING PRIMARY KEY,
                    enabled BOOLEAN,
                    failure_type STRING,
                    failure_next_epoch NUMBER(38,0),
                    effective_from_epoch NUMBER(38,0)
                )
            """)
            execute_write_sql(f"""
                MERGE INTO {STREAM_STATE} t
                USING (SELECT '{STREAM_NAME}' AS stream_name) s
                ON t.stream_name = s.stream_name
                WHEN NOT MATCHED THEN INSERT (stream_name, start_ts, step_seconds, next_epoch)
                VALUES ('{STREAM_NAME}', CURRENT_TIMESTAMP(), 5, 0)
            """)
            execute_write_sql(f"""
                UPDATE {STREAM_STATE}
                SET start_ts = COALESCE(start_ts, CURRENT_TIMESTAMP()),
                    step_seconds = COALESCE(step_seconds, 5),
                    next_epoch = COALESCE(next_epoch, 0)
                WHERE stream_name = '{STREAM_NAME}'
            """)
        finally:
            write_lock.release()
        return {"status": "success", "message": "Database initialized"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/telemetry/latest")
def get_latest_telemetry():
    current_time = time.time()
    with cache_lock:
        if telemetry_cache["data"] is not None and (current_time - telemetry_cache["timestamp"]) < CACHE_TTL:
            return JSONResponse(content=telemetry_cache["data"])
    query = f"""
    WITH stream AS (
      SELECT NEXT_EPOCH, start_ts, step_seconds FROM {STREAM_STATE} WHERE STREAM_NAME = '{STREAM_NAME}'
    ),
    latest_data AS (
      SELECT entity_id, timestamp, engine_temp, trans_oil_pressure, battery_voltage,
        MAX(timestamp) OVER () as global_max_timestamp
      FROM {TELEMETRY}
      WHERE timestamp >= (SELECT DATEADD(second, GREATEST(s.NEXT_EPOCH - 2, 0) * s.step_seconds, s.start_ts) FROM stream s)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY timestamp DESC) = 1
    ),
    all_entities AS (SELECT DISTINCT entity_id FROM {NORMAL_SEED}),
    broken_down AS (
      SELECT ENTITY_ID FROM {FIRST_FAILURE_MARKERS}
      WHERE BREAKDOWN_EPOCH IS NOT NULL AND BREAKDOWN_EPOCH <= (SELECT NEXT_EPOCH FROM stream)
    )
    SELECT ae.entity_id,
      CASE
        WHEN bd.ENTITY_ID IS NOT NULL THEN 'OFFLINE'
        WHEN ld.timestamp IS NULL THEN 'OFFLINE'
        WHEN DATEDIFF('second', ld.timestamp, ld.global_max_timestamp) <= 10 THEN 'ONLINE'
        ELSE 'OFFLINE'
      END as status,
      ld.timestamp, ld.engine_temp, ld.trans_oil_pressure, ld.battery_voltage
    FROM all_entities ae
    LEFT JOIN latest_data ld ON ae.entity_id = ld.entity_id
    LEFT JOIN broken_down bd ON ae.entity_id = bd.ENTITY_ID
    ORDER BY ae.entity_id
    """
    df = execute_query(query)
    result = json.loads(df.to_json(orient='records', date_format='iso'))
    with cache_lock:
        telemetry_cache["data"] = result
        telemetry_cache["timestamp"] = current_time
    return JSONResponse(content=result)


@app.get("/api/fleet-status")
def get_fleet_status():
    current_time = time.time()
    with cache_lock:
        if telemetry_cache["data"] is not None and telemetry_cache.get("fleet") and (current_time - telemetry_cache["timestamp"]) < CACHE_TTL:
            return JSONResponse(content=telemetry_cache["fleet"])
    query = f"""
    WITH stream AS (
      SELECT NEXT_EPOCH, start_ts, step_seconds FROM {STREAM_STATE} WHERE STREAM_NAME = '{STREAM_NAME}'
    ),
    latest_data AS (
      SELECT entity_id, timestamp, engine_temp, trans_oil_pressure, battery_voltage,
        MAX(timestamp) OVER () as global_max_timestamp
      FROM {TELEMETRY}
      WHERE timestamp >= (SELECT DATEADD(second, GREATEST(s.NEXT_EPOCH - 2, 0) * s.step_seconds, s.start_ts) FROM stream s)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY timestamp DESC) = 1
    ),
    all_entities AS (SELECT DISTINCT entity_id FROM {NORMAL_SEED}),
    broken_down AS (
      SELECT ENTITY_ID FROM {FIRST_FAILURE_MARKERS}
      WHERE BREAKDOWN_EPOCH IS NOT NULL AND BREAKDOWN_EPOCH <= (SELECT NEXT_EPOCH FROM stream)
    )
    SELECT ae.entity_id,
      CASE
        WHEN bd.ENTITY_ID IS NOT NULL THEN 'OFFLINE'
        WHEN ld.timestamp IS NULL THEN 'OFFLINE'
        WHEN DATEDIFF('second', ld.timestamp, ld.global_max_timestamp) <= 10 THEN 'ONLINE'
        ELSE 'OFFLINE'
      END as status,
      ld.timestamp, ld.engine_temp, ld.trans_oil_pressure, ld.battery_voltage
    FROM all_entities ae
    LEFT JOIN latest_data ld ON ae.entity_id = ld.entity_id
    LEFT JOIN broken_down bd ON ae.entity_id = bd.ENTITY_ID
    ORDER BY ae.entity_id
    """
    df = execute_query(query)
    result = json.loads(df.to_json(orient='records', date_format='iso'))
    with cache_lock:
        telemetry_cache["fleet"] = result
        telemetry_cache["timestamp"] = current_time
    return JSONResponse(content=result)


@app.get("/api/predictions/latest")
def get_latest_predictions():
    current_time = time.time()
    with cache_lock:
        if predictions_cache["data"] is not None and (current_time - predictions_cache["timestamp"]) < PREDICTIONS_CACHE_TTL:
            return JSONResponse(content=predictions_cache["data"], headers={"Cache-Control": "no-cache"})
    query = f"""
    WITH stream AS (
      SELECT NEXT_EPOCH, start_ts, step_seconds FROM {STREAM_STATE} WHERE STREAM_NAME = '{STREAM_NAME}'
    ),
    latest_telemetry AS (
      SELECT MAX(TIMESTAMP) as LATEST_TELEMETRY_TIME FROM {TELEMETRY}
      WHERE timestamp >= (SELECT DATEADD(second, GREATEST(s.NEXT_EPOCH - 2, 0) * s.step_seconds, s.start_ts) FROM stream s)
    ),
    cached_predictions AS (
        SELECT ENTITY_ID, PREDICTION_TIMESTAMP, PREDICTED_FAILURE_TYPE,
            PREDICTED_HOURS_TO_FAILURE, TTF_MODEL_USED,
            CURRENT_ENGINE_TEMP, CURRENT_TRANS_PRESSURE, CURRENT_BATTERY_VOLTAGE, LAST_UPDATED
        FROM {PREDICTION_CACHE}
    )
    SELECT p.ENTITY_ID, p.PREDICTION_TIMESTAMP, p.PREDICTED_FAILURE_TYPE,
        p.PREDICTED_HOURS_TO_FAILURE, p.TTF_MODEL_USED, p.LAST_UPDATED,
        t.LATEST_TELEMETRY_TIME,
        CASE WHEN t.LATEST_TELEMETRY_TIME >= p.PREDICTION_TIMESTAMP
            THEN DATEDIFF(MINUTE, p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME) ELSE 0
        END as AGE_MINUTES,
        CASE
            WHEN t.LATEST_TELEMETRY_TIME >= p.PREDICTION_TIMESTAMP
                AND DATEDIFF(MINUTE, p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME) <= 5 THEN 'green'
            WHEN t.LATEST_TELEMETRY_TIME >= p.PREDICTION_TIMESTAMP
                AND DATEDIFF(MINUTE, p.PREDICTION_TIMESTAMP, t.LATEST_TELEMETRY_TIME) < 60 THEN 'orange'
            WHEN t.LATEST_TELEMETRY_TIME < p.PREDICTION_TIMESTAMP THEN 'green'
            ELSE 'red'
        END as AGE_COLOR,
        CONCAT(p.ENTITY_ID, '_', TO_VARCHAR(p.LAST_UPDATED, 'YYYY-MM-DD HH24:MI:SS.FF3')) as PREDICTION_KEY
    FROM cached_predictions p
    CROSS JOIN latest_telemetry t
    ORDER BY p.ENTITY_ID
    """
    try:
        df = execute_query(query)
        if df.empty:
            return JSONResponse(content=[])
        max_age = df['AGE_MINUTES'].max()
        if max_age >= 60:
            thread = threading.Thread(target=trigger_refresh_sync)
            thread.start()
        response_data = json.loads(df.to_json(orient='records', date_format='iso'))
        with cache_lock:
            predictions_cache["data"] = response_data
            predictions_cache["timestamp"] = current_time
        return JSONResponse(content=response_data, headers={"Cache-Control": "no-cache"})
    except Exception as e:
        logger.error(f"Error fetching predictions: {e}")
        return JSONResponse(content=[])


def trigger_refresh_sync():
    global refresh_in_progress, last_refresh_time
    current_time = time.time()
    if current_time - last_refresh_time < 10:
        return
    if refresh_in_progress:
        return
    with refresh_lock:
        refresh_in_progress = True
        last_refresh_time = current_time
        try:
            logger.info("Background refresh starting...")
            acquired = write_lock.acquire(timeout=30)
            if not acquired:
                logger.warning("trigger_refresh_sync: could not acquire write_lock")
                return
            try:
                execute_write_sql(f"""
                    MERGE INTO {PREDICTION_CACHE} AS target
                    USING (
                        SELECT ENTITY_ID, PREDICTION_TIMESTAMP, PREDICTED_FAILURE_TYPE,
                            PREDICTED_HOURS_TO_FAILURE, TTF_MODEL_USED,
                            CURRENT_ENGINE_TEMP, CURRENT_TRANS_PRESSURE, CURRENT_BATTERY_VOLTAGE
                        FROM {ENHANCED_PREDICTIVE_VIEW_HYBRID_TTF}
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY ENTITY_ID ORDER BY PREDICTION_TIMESTAMP DESC) = 1
                    ) AS source
                    ON target.ENTITY_ID = source.ENTITY_ID
                    WHEN MATCHED THEN UPDATE SET
                        PREDICTION_TIMESTAMP = source.PREDICTION_TIMESTAMP,
                        PREDICTED_FAILURE_TYPE = source.PREDICTED_FAILURE_TYPE,
                        PREDICTED_HOURS_TO_FAILURE = source.PREDICTED_HOURS_TO_FAILURE,
                        TTF_MODEL_USED = source.TTF_MODEL_USED,
                        CURRENT_ENGINE_TEMP = source.CURRENT_ENGINE_TEMP,
                        CURRENT_TRANS_PRESSURE = source.CURRENT_TRANS_PRESSURE,
                        CURRENT_BATTERY_VOLTAGE = source.CURRENT_BATTERY_VOLTAGE,
                        LAST_UPDATED = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        ENTITY_ID, PREDICTION_TIMESTAMP, PREDICTED_FAILURE_TYPE,
                        PREDICTED_HOURS_TO_FAILURE, TTF_MODEL_USED,
                        CURRENT_ENGINE_TEMP, CURRENT_TRANS_PRESSURE, CURRENT_BATTERY_VOLTAGE, LAST_UPDATED
                    ) VALUES (
                        source.ENTITY_ID, source.PREDICTION_TIMESTAMP, source.PREDICTED_FAILURE_TYPE,
                        source.PREDICTED_HOURS_TO_FAILURE, source.TTF_MODEL_USED,
                        source.CURRENT_ENGINE_TEMP, source.CURRENT_TRANS_PRESSURE,
                        source.CURRENT_BATTERY_VOLTAGE, CURRENT_TIMESTAMP()
                    )
                """)
                schema = SCHEMA_PREFIX
                execute_write_sql(f"""
                    INSERT INTO {FIRST_FAILURE_MARKERS} (ENTITY_ID, FIRST_FAILURE_TIME, FAILURE_TYPE, LAST_UPDATED)
                    SELECT p.ENTITY_ID,
                        TIME_SLICE(p.PREDICTION_TIMESTAMP, 5, 'MINUTE', 'START'),
                        p.PREDICTED_FAILURE_TYPE, CURRENT_TIMESTAMP()
                    FROM {PREDICTION_CACHE} p
                    WHERE p.PREDICTED_FAILURE_TYPE != 'NORMAL'
                    AND NOT EXISTS (
                        SELECT 1 FROM {FIRST_FAILURE_MARKERS} m
                        WHERE m.ENTITY_ID = p.ENTITY_ID AND m.FAILURE_TYPE = p.PREDICTED_FAILURE_TYPE
                    );
                    DELETE FROM {FIRST_FAILURE_MARKERS}
                    WHERE ENTITY_ID NOT IN (
                        SELECT ENTITY_ID FROM {PREDICTION_CACHE} WHERE PREDICTED_FAILURE_TYPE != 'NORMAL'
                    );
                    UPDATE {FIRST_FAILURE_MARKERS} fm
                    SET BREAKDOWN_EPOCH = (SELECT NEXT_EPOCH FROM {schema}.STREAM_STATE WHERE STREAM_NAME = '{STREAM_NAME}')
                    WHERE fm.BREAKDOWN_EPOCH IS NULL
                    AND EXISTS (
                        SELECT 1 FROM {PREDICTION_CACHE} p
                        WHERE p.ENTITY_ID = fm.ENTITY_ID AND p.PREDICTED_HOURS_TO_FAILURE <= 0.5
                    )
                """, num_statements=3)
            finally:
                write_lock.release()
            logger.info("Background refresh completed")
            with cache_lock:
                predictions_cache["timestamp"] = 0
                markers_cache["timestamp"] = 0
        except Exception as e:
            logger.error(f"Background refresh failed: {e}")
        finally:
            refresh_in_progress = False


@app.post("/api/predictions/refresh")
def refresh_predictions():
    global refresh_in_progress, last_refresh_time, pending_refresh_thread
    current_time = time.time()
    if current_time - last_refresh_time < 10:
        elapsed = int(current_time - last_refresh_time)
        return {"status": "throttled", "message": f"Please wait {10 - elapsed} seconds"}
    if refresh_in_progress:
        return {"status": "in_progress", "message": "Refresh already running"}
    if pending_refresh_thread is None or not pending_refresh_thread.is_alive():
        pending_refresh_thread = threading.Thread(target=trigger_refresh_sync, daemon=True)
        pending_refresh_thread.start()
    return {"status": "success"}


@app.post("/api/writer/write-epoch")
def write_epoch():
    try:
        acquired = write_lock.acquire(timeout=15)
        if not acquired:
            logger.warning("write_epoch: could not acquire write_lock within 15s")
            return {"status": "error", "message": "Write lock timeout"}
        try:
            st_row = execute_write_query(f"SELECT start_ts, step_seconds, next_epoch FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}'")
            if st_row.empty:
                return {"status": "error", "message": "Stream state not found"}
            step_seconds = int(st_row.iloc[0]["STEP_SECONDS"]) or 5
            current_epoch = int(st_row.iloc[0]["NEXT_EPOCH"])
            target_epoch = current_epoch + 1

            execute_write_sql(f"""
                INSERT INTO {TELEMETRY} (Timestamp, entity_id, engine_temp, trans_oil_pressure, battery_voltage)
                WITH stream_info AS (
                    SELECT start_ts, step_seconds, {target_epoch} AS target_epoch
                    FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}'
                ),
                broken_down_trucks AS (
                    SELECT ENTITY_ID FROM {FIRST_FAILURE_MARKERS}
                    WHERE BREAKDOWN_EPOCH IS NOT NULL AND BREAKDOWN_EPOCH <= {target_epoch}
                ),
                active_failures AS (
                    SELECT fc.entity_id, fc.failure_type,
                           COALESCE(fc.failure_next_epoch, 0) + 1 AS next_failure_epoch
                    FROM {FAILURE_CONFIG} fc, stream_info si
                    WHERE fc.enabled = true AND fc.effective_from_epoch <= si.target_epoch
                      AND fc.entity_id NOT IN (SELECT ENTITY_ID FROM broken_down_trucks)
                ),
                normal_data AS (
                    SELECT DATEADD(second, si.target_epoch * si.step_seconds, si.start_ts) AS ts,
                        n.entity_id, n.engine_temp, n.trans_oil_pressure, n.battery_voltage
                    FROM stream_info si
                    CROSS JOIN {NORMAL_SEED} n
                    LEFT JOIN active_failures af ON af.entity_id = n.entity_id
                    WHERE n.epoch = si.target_epoch AND af.entity_id IS NULL
                      AND n.entity_id NOT IN (SELECT ENTITY_ID FROM broken_down_trucks)
                ),
                failure_data AS (
                    SELECT DATEADD(second, si.target_epoch * si.step_seconds, si.start_ts) AS ts,
                        af.entity_id,
                        COALESCE(ef.engine_temp, tf.engine_temp, el.engine_temp) AS engine_temp,
                        COALESCE(ef.trans_oil_pressure, tf.trans_oil_pressure, el.trans_oil_pressure) AS trans_oil_pressure,
                        COALESCE(ef.battery_voltage, tf.battery_voltage, el.battery_voltage) AS battery_voltage
                    FROM stream_info si
                    CROSS JOIN active_failures af
                    LEFT JOIN {ENGINE_FAILURE_SEED} ef ON af.failure_type = 'ENGINE' AND ef.epoch = af.next_failure_epoch
                    LEFT JOIN {TRANSMISSION_FAILURE_SEED} tf ON af.failure_type = 'TRANSMISSION' AND tf.epoch = af.next_failure_epoch
                    LEFT JOIN {ELECTRICAL_FAILURE_SEED} el ON af.failure_type = 'ELECTRICAL' AND el.epoch = af.next_failure_epoch
                    WHERE COALESCE(ef.epoch, tf.epoch, el.epoch) IS NOT NULL
                )
                SELECT ts, entity_id, engine_temp, trans_oil_pressure, battery_voltage
                FROM normal_data
                UNION ALL
                SELECT ts, entity_id, engine_temp, trans_oil_pressure, battery_voltage
                FROM failure_data
            """)

            execute_write_sql(f"""
                UPDATE {FAILURE_CONFIG} fc
                SET failure_next_epoch = COALESCE(fc.failure_next_epoch, 0) + 1
                WHERE fc.enabled = true AND fc.effective_from_epoch <= {target_epoch}
                  AND EXISTS (
                      SELECT 1 FROM (
                          SELECT 'ENGINE' AS ft, epoch FROM {ENGINE_FAILURE_SEED}
                          UNION ALL SELECT 'TRANSMISSION', epoch FROM {TRANSMISSION_FAILURE_SEED}
                          UNION ALL SELECT 'ELECTRICAL', epoch FROM {ELECTRICAL_FAILURE_SEED}
                      ) seeds WHERE seeds.ft = fc.failure_type AND seeds.epoch = COALESCE(fc.failure_next_epoch, 0) + 1
                  );
                UPDATE {STREAM_STATE} SET next_epoch = {target_epoch} WHERE stream_name = '{STREAM_NAME}'
            """, num_statements=2)
            maybe_prune_telemetry()
        finally:
            write_lock.release()
        with cache_lock:
            telemetry_cache["timestamp"] = 0
        logger.info(f"Epoch {target_epoch} written")
        return {"status": "success", "epoch": target_epoch}
    except Exception as e:
        logger.error(f"Write epoch failed: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/api/fast-forward/{hours}")
def fast_forward(hours: int):
    try:
        logger.info(f"Fast forward requested: {hours} hours")
        acquired = write_lock.acquire(timeout=60)
        if not acquired:
            logger.warning("fast_forward: could not acquire write_lock within 60s")
            return {"status": "error", "message": "Write lock timeout"}
        try:
            st_row = execute_write_query(f"select start_ts, step_seconds, next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
            if st_row.empty:
                return {"status": "error", "message": "Stream state not found"}
            step_seconds = int(st_row.iloc[0]["STEP_SECONDS"]) or 5
            ne = int(st_row.iloc[0]["NEXT_EPOCH"])
            epochs_to_write = int((hours * 3600) // step_seconds)
            if epochs_to_write <= 0:
                return {"status": "error", "message": "No epochs to write"}

            cfg_df = execute_write_query(f"""
                SELECT entity_id, failure_type, failure_next_epoch, effective_from_epoch
                FROM {FAILURE_CONFIG} WHERE enabled = true
            """)
            failure_entities = cfg_df['ENTITY_ID'].tolist() if not cfg_df.empty else []
            in_filter = ""
            if failure_entities:
                ids = ",".join([f"'{e.replace(chr(39), chr(39)+chr(39))}'" for e in failure_entities])
                in_filter = f" AND n.entity_id NOT IN ({ids})"

            execute_write_sql(f"""
                INSERT INTO {TELEMETRY} (Timestamp, entity_id, engine_temp, trans_oil_pressure, battery_voltage)
                SELECT DATEADD(second, (st.ne + g.seq + 1) * st.step_seconds, st.start_ts) as ts,
                    n.entity_id, n.engine_temp, n.trans_oil_pressure, n.battery_voltage
                FROM (SELECT next_epoch as ne, start_ts, step_seconds FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}') st
                CROSS JOIN (SELECT seq4() as seq FROM table(generator(rowcount => {epochs_to_write}))) g
                JOIN {NORMAL_SEED} n ON n.epoch = st.ne + g.seq + 1
                WHERE 1=1{in_filter}
            """)

            cursor_updates = []
            failure_inserts = []
            for _, row in cfg_df.iterrows():
                eid = row['ENTITY_ID']
                ftype = row['FAILURE_TYPE']
                cur = int(row['FAILURE_NEXT_EPOCH']) if row['FAILURE_NEXT_EPOCH'] is not None else 0
                eff = int(row['EFFECTIVE_FROM_EPOCH']) if row['EFFECTIVE_FROM_EPOCH'] is not None else 0
                seed_table = {
                    'ENGINE': ENGINE_FAILURE_SEED,
                    'TRANSMISSION': TRANSMISSION_FAILURE_SEED,
                    'ELECTRICAL': ELECTRICAL_FAILURE_SEED
                }.get(ftype, ELECTRICAL_FAILURE_SEED)
                eid_esc = eid.replace("'", "''")
                failure_inserts.append(f"""
                    SELECT DATEADD(second, (st.ne + g.seq + 1) * st.step_seconds, st.start_ts) as ts,
                        '{eid_esc}' as entity_id, f.engine_temp, f.trans_oil_pressure, f.battery_voltage
                    FROM (SELECT next_epoch as ne, start_ts, step_seconds FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}') st
                    CROSS JOIN (SELECT seq4() as seq FROM table(generator(rowcount => {epochs_to_write}))) g
                    JOIN {seed_table} f ON f.epoch = {cur} + (g.seq + 1 - GREATEST(0, {eff} - (st.ne + 1)))
                    WHERE (st.ne + g.seq + 1) >= {eff}
                """)
                if ne + epochs_to_write >= eff:
                    adv = min(epochs_to_write, ne + epochs_to_write - eff + 1) if eff > ne else epochs_to_write
                    if adv > 0:
                        cursor_updates.append((eid_esc, adv))

            if failure_inserts:
                combined = " UNION ALL ".join(failure_inserts)
                execute_write_sql(f"""
                    INSERT INTO {TELEMETRY} (Timestamp, entity_id, engine_temp, trans_oil_pressure, battery_voltage)
                    {combined}
                """)

            final_stmts = []
            if cursor_updates:
                when_clauses = "\n".join([f"WHEN entity_id = '{eid}' THEN COALESCE(failure_next_epoch, 0) + {adv}" for eid, adv in cursor_updates])
                entity_list = ",".join([f"'{eid}'" for eid, _ in cursor_updates])
                final_stmts.append(f"UPDATE {FAILURE_CONFIG} SET failure_next_epoch = CASE {when_clauses} END WHERE entity_id IN ({entity_list})")

            try:
                predictions_df = execute_write_query(f"""
                    SELECT ENTITY_ID, PREDICTED_HOURS_TO_FAILURE
                    FROM {PREDICTION_CACHE}
                    WHERE PREDICTED_FAILURE_TYPE != 'NORMAL' AND PREDICTED_HOURS_TO_FAILURE IS NOT NULL AND PREDICTED_HOURS_TO_FAILURE > 0
                """)
                if not predictions_df.empty:
                    epochs_per_hour = 3600 / step_seconds
                    breakdown_cases = []
                    breakdown_ids = []
                    for _, prow in predictions_df.iterrows():
                        entity_id = prow['ENTITY_ID']
                        ttf_hours = float(prow['PREDICTED_HOURS_TO_FAILURE'])
                        if ttf_hours <= hours:
                            breakdown_epoch = ne + int(ttf_hours * epochs_per_hour)
                            entity_esc = entity_id.replace("'", "''")
                            breakdown_cases.append(f"WHEN ENTITY_ID = '{entity_esc}' THEN {breakdown_epoch}")
                            breakdown_ids.append(f"'{entity_esc}'")
                    if breakdown_cases:
                        cases_sql = "\n".join(breakdown_cases)
                        ids_sql = ",".join(breakdown_ids)
                        final_stmts.append(f"""
                            UPDATE {FIRST_FAILURE_MARKERS} SET BREAKDOWN_EPOCH = CASE {cases_sql} END
                            WHERE ENTITY_ID IN ({ids_sql}) AND BREAKDOWN_EPOCH IS NULL
                        """)
            except Exception as be:
                logger.warning(f"Breakdown epoch calculation failed: {be}")

            final_stmts.append(f"UPDATE {STREAM_STATE} SET next_epoch = next_epoch + {epochs_to_write} WHERE stream_name = '{STREAM_NAME}'")
            if len(final_stmts) > 1:
                execute_write_sql(";\n".join(final_stmts), num_statements=len(final_stmts))
            else:
                execute_write_sql(final_stmts[0])
            maybe_prune_telemetry()
        finally:
            write_lock.release()

        with cache_lock:
            telemetry_cache["timestamp"] = 0
            predictions_cache["timestamp"] = 0
            failures_cache["timestamp"] = 0
            chart_cache["timestamp"] = 0
            markers_cache["timestamp"] = 0
            map_trucks_cache["timestamp"] = 0
            map_trucks_cache["epoch"] = 0
        service_rec_cache.clear()

        if hours >= 1:
            global pending_refresh_thread
            if pending_refresh_thread is None or not pending_refresh_thread.is_alive():
                pending_refresh_thread = threading.Thread(target=trigger_refresh_sync, daemon=True)
                pending_refresh_thread.start()

        return {"status": "success", "epochs_inserted": epochs_to_write}
    except Exception as e:
        logger.error(f"Fast forward failed: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/api/failure/activate")
def activate_failure_endpoint(entity_id: str, failure_type: str):
    try:
        ft = failure_type.upper()
        eid_esc = entity_id.replace("'", "''")
        st_row = execute_query(f"select next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
        ne = int(st_row.iloc[0]["NEXT_EPOCH"]) if not st_row.empty else 0
        eff = ne + 1
        acquired = write_lock.acquire(timeout=15)
        if not acquired:
            return {"status": "error", "message": "Write lock timeout"}
        try:
            execute_write_sql(f"""
                merge into {FAILURE_CONFIG} t using (select '{eid_esc}' as entity_id) s on t.entity_id = s.entity_id
                when matched then update set enabled = true, failure_type = '{ft}', failure_next_epoch = coalesce(t.failure_next_epoch, 0), effective_from_epoch = {eff}
                when not matched then insert(entity_id, enabled, failure_type, failure_next_epoch, effective_from_epoch) values('{eid_esc}', true, '{ft}', 0, {eff})
            """)
        finally:
            write_lock.release()
        with cache_lock:
            failures_cache["timestamp"] = 0
            telemetry_cache["timestamp"] = 0
            map_trucks_cache["timestamp"] = 0
        return {"status": "success", "message": f"{ft} failure activated for {entity_id}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/failures/active")
def get_active_failures():
    current_time = time.time()
    with cache_lock:
        if failures_cache["data"] is not None and (current_time - failures_cache["timestamp"]) < FAILURES_CACHE_TTL:
            return JSONResponse(
                content=failures_cache["data"],
                headers={"Cache-Control": "no-cache, no-store", "X-Cache": "HIT"}
            )
    try:
        query = f"""
        WITH failure_seed_sizes AS (
            SELECT 'ENGINE' as failure_type, COUNT(*) as max_epochs FROM {ENGINE_FAILURE_SEED}
            UNION ALL SELECT 'TRANSMISSION', COUNT(*) FROM {TRANSMISSION_FAILURE_SEED}
            UNION ALL SELECT 'ELECTRICAL', COUNT(*) FROM {ELECTRICAL_FAILURE_SEED}
        )
        SELECT fc.entity_id, fc.failure_type, fc.effective_from_epoch,
            fc.failure_next_epoch as current_failure_epoch, fss.max_epochs,
            CASE WHEN fc.failure_next_epoch > fss.max_epochs THEN 'OFFLINE' ELSE 'ACTIVE' END as status
        FROM {FAILURE_CONFIG} fc
        JOIN failure_seed_sizes fss ON fss.failure_type = fc.failure_type
        WHERE fc.enabled = true ORDER BY fc.entity_id
        """
        df = execute_query(query)
        result = json.loads(df.to_json(orient='records'))
        with cache_lock:
            failures_cache["data"] = result
            failures_cache["timestamp"] = current_time
        return JSONResponse(
            content=result,
            headers={"Cache-Control": "no-cache, no-store", "X-Cache": "MISS"}
        )
    except Exception as e:
        with cache_lock:
            if failures_cache["data"] is not None:
                return JSONResponse(
                    content=failures_cache["data"],
                    headers={"Cache-Control": "no-cache, no-store", "X-Cache": "STALE"}
                )
        return JSONResponse(content=[])


@app.delete("/api/failure/clear")
def clear_failures():
    try:
        acquired = write_lock.acquire(timeout=15)
        if not acquired:
            return {"status": "error", "message": "Write lock timeout"}
        try:
            execute_write_sql(f"DELETE FROM {FAILURE_CONFIG}")
        finally:
            write_lock.release()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/chart-data/{hours}")
def get_chart_data(hours: int):
    current_time = time.time()
    with cache_lock:
        if chart_cache["data"] is not None and chart_cache.get("hours") == hours and (current_time - chart_cache["timestamp"]) < CACHE_TTL:
            return JSONResponse(content=chart_cache["data"])
    try:
        try:
            row_limit = max(2000, hours * 12 * 10 + 500)
            query = f"""
            SELECT TO_CHAR(bucket_time, 'YYYY-MM-DD HH24:MI:SS') as TIMESTAMP,
                entity_id as ENTITY_ID, avg_engine_temp as ENGINE_TEMP,
                avg_trans_oil_pressure as TRANS_OIL_PRESSURE, avg_battery_voltage as BATTERY_VOLTAGE
            FROM {TELEMETRY_5MIN_AGG}
            WHERE bucket_time >= DATEADD(hour, -{hours}, (SELECT MAX(bucket_time) FROM {TELEMETRY_5MIN_AGG}))
            ORDER BY bucket_time ASC LIMIT {row_limit}
            """
            df = execute_query(query)
            if not df.empty:
                chart_result = json.loads(df.to_json(orient='records'))
                with cache_lock:
                    chart_cache["data"] = chart_result
                    chart_cache["timestamp"] = current_time
                    chart_cache["hours"] = hours
                return JSONResponse(content=chart_result)
        except:
            pass
        row_limit = max(2000, hours * 12 * 10 + 500)
        fallback_query = f"""
        SELECT TO_CHAR(TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), 'YYYY-MM-DD HH24:MI:SS') as TIMESTAMP,
            ENTITY_ID, AVG(ENGINE_TEMP) as ENGINE_TEMP,
            AVG(TRANS_OIL_PRESSURE) as TRANS_OIL_PRESSURE, AVG(BATTERY_VOLTAGE) as BATTERY_VOLTAGE
        FROM {TELEMETRY}
        WHERE TIMESTAMP >= DATEADD(hour, -{hours}, (SELECT MAX(TIMESTAMP) FROM {TELEMETRY}))
        GROUP BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), ENTITY_ID
        ORDER BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END') ASC LIMIT {row_limit}
        """
        df = execute_query(fallback_query)
        chart_result = json.loads(df.to_json(orient='records'))
        with cache_lock:
            chart_cache["data"] = chart_result
            chart_cache["timestamp"] = current_time
            chart_cache["hours"] = hours
        return JSONResponse(content=chart_result)
    except Exception as e:
        return JSONResponse(content=[])


@app.get("/api/predictions/first-failure-markers")
def get_first_failure_markers():
    current_time = time.time()
    with cache_lock:
        if markers_cache["data"] is not None and (current_time - markers_cache["timestamp"]) < CACHE_TTL:
            return JSONResponse(content=markers_cache["data"])
    try:
        query = f"""
        SELECT ENTITY_ID, TO_CHAR(FIRST_FAILURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as FIRST_FAILURE_TIME, FAILURE_TYPE
        FROM {FIRST_FAILURE_MARKERS} ORDER BY ENTITY_ID
        """
        df = execute_query(query)
        result = json.loads(df.to_json(orient='records'))
        with cache_lock:
            markers_cache["data"] = result
            markers_cache["timestamp"] = current_time
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content=[])


@app.post("/api/reset")
@app.get("/api/reset")
@app.post("/api/reset-all")
@app.get("/api/reset-all")
def reset_all_data():
    try:
        logger.info("FULL RESET - clearing all data...")
        acquired = write_lock.acquire(timeout=30)
        if not acquired:
            return {"status": "error", "message": "Write lock timeout"}
        try:
            reset_sql = (
                f"TRUNCATE TABLE IF EXISTS {TELEMETRY};"
                f"TRUNCATE TABLE IF EXISTS {FIRST_FAILURE_MARKERS};"
                f"DELETE FROM {ACTIVE_FAILURES};"
                f"TRUNCATE TABLE IF EXISTS {PREDICTION_CACHE};"
                f"TRUNCATE TABLE IF EXISTS {FAILURE_CONFIG};"
                f"DELETE FROM {STREAM_STATE} WHERE stream_name = '{STREAM_NAME}';"
                f"INSERT INTO {STREAM_STATE} (stream_name, start_ts, step_seconds, next_epoch)"
                f" VALUES ('{STREAM_NAME}', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 5, 0);"
            )
            execute_write_sql(reset_sql, num_statements=7)
        finally:
            write_lock.release()
        with cache_lock:
            telemetry_cache["timestamp"] = 0
            predictions_cache["timestamp"] = 0
            failures_cache["timestamp"] = 0
            chart_cache["timestamp"] = 0
            markers_cache["timestamp"] = 0
            map_trucks_cache["timestamp"] = 0
            map_trucks_cache["epoch"] = 0
        service_rec_cache.clear()
        return {"status": "success", "message": "Complete reset - all data cleared"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/writer/status")
def get_writer_status():
    try:
        df = execute_query(f"select start_ts, step_seconds, next_epoch from {STREAM_STATE} where stream_name = '{STREAM_NAME}'")
        if df.empty:
            return {"status": "not_initialized"}
        return {"status": "initialized", "epoch": int(df.iloc[0]["NEXT_EPOCH"]), "step_seconds": int(df.iloc[0]["STEP_SECONDS"])}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/api/writer/start")
async def start_writer():
    return {"status": "started"}

@app.post("/api/writer/stop")
async def stop_writer():
    return {"status": "stopped"}


@app.get("/api/map/trucks")
def get_map_trucks():
    global map_trucks_cache
    current_time = time.time()
    try:
        schema = SCHEMA_PREFIX or "NEW_FTFP.DATA"
        epoch_df = execute_query(f"SELECT NEXT_EPOCH FROM {schema}.STREAM_STATE WHERE STREAM_NAME = '{STREAM_NAME}'")
        current_epoch = int(epoch_df.iloc[0]['NEXT_EPOCH']) if not epoch_df.empty else 1
        with cache_lock:
            if (map_trucks_cache["data"] is not None and
                map_trucks_cache["epoch"] == current_epoch and
                (current_time - map_trucks_cache["timestamp"]) < MAP_CACHE_TTL):
                return map_trucks_cache["data"]

        df = execute_query(f"""
            WITH broken_down AS (
                SELECT DISTINCT fm.ENTITY_ID, fm.BREAKDOWN_EPOCH
                FROM {schema}.FIRST_FAILURE_MARKERS fm
                WHERE fm.BREAKDOWN_EPOCH IS NOT NULL
            ),
            route_info AS (
                SELECT ENTITY_ID, MAX(MILES_FROM_START) AS ACTUAL_ROUTE_MILES, MAX(EPOCH) AS MAX_EPOCH
                FROM {schema}.ROUTE_WAYPOINTS GROUP BY ENTITY_ID
            ),
            truck_progress AS (
                SELECT r.ENTITY_ID, r.ROUTE_NAME, r.START_CITY, r.END_CITY,
                    ri.ACTUAL_ROUTE_MILES AS ROUTE_MILES, ri.MAX_EPOCH,
                    CASE WHEN bd.ENTITY_ID IS NOT NULL THEN bd.BREAKDOWN_EPOCH * {MILES_PER_EPOCH}
                        ELSE {current_epoch} * {MILES_PER_EPOCH}
                    END AS TOTAL_MILES_DRIVEN,
                    bd.ENTITY_ID IS NOT NULL AS IS_BROKEN_DOWN
                FROM {schema}.TRUCK_ROUTES r
                JOIN route_info ri ON r.ENTITY_ID = ri.ENTITY_ID
                LEFT JOIN broken_down bd ON r.ENTITY_ID = bd.ENTITY_ID
            ),
            with_legs AS (
                SELECT tp.*,
                    FLOOR(tp.TOTAL_MILES_DRIVEN / tp.ROUTE_MILES) AS LEGS_COMPLETED,
                    MOD(tp.TOTAL_MILES_DRIVEN, tp.ROUTE_MILES) AS MILES_THIS_LEG,
                    MOD(FLOOR(tp.TOTAL_MILES_DRIVEN / tp.ROUTE_MILES), 2) = 1 AS IS_RETURN_TRIP
                FROM truck_progress tp
            ),
            with_target_miles AS (
                SELECT wl.*, wl.MILES_THIS_LEG AS EFFECTIVE_MILES,
                    CASE WHEN wl.IS_RETURN_TRIP THEN wl.ROUTE_MILES - wl.MILES_THIS_LEG
                        ELSE wl.MILES_THIS_LEG END AS TARGET_MILES
                FROM with_legs wl
            ),
            matched_waypoints AS (
                SELECT wtm.*, w.EPOCH, w.LATITUDE, w.LONGITUDE, w.HEADING, w.MILES_FROM_START,
                    ABS(w.MILES_FROM_START - wtm.TARGET_MILES) AS MILES_DIFF,
                    ROW_NUMBER() OVER (PARTITION BY wtm.ENTITY_ID ORDER BY ABS(w.MILES_FROM_START - wtm.TARGET_MILES)) AS rn
                FROM with_target_miles wtm
                JOIN {schema}.ROUTE_WAYPOINTS w ON wtm.ENTITY_ID = w.ENTITY_ID
            ),
            predictions AS (
                SELECT ENTITY_ID,
                    COALESCE(PREDICTED_FAILURE_TYPE, 'NORMAL') AS PREDICTED_FAILURE_TYPE,
                    COALESCE(PREDICTED_HOURS_TO_FAILURE, -1) AS PREDICTED_HOURS_TO_FAILURE
                FROM {schema}.PREDICTION_CACHE
            )
            SELECT mw.ENTITY_ID, mw.LATITUDE, mw.LONGITUDE,
                CASE WHEN mw.IS_RETURN_TRIP THEN MOD(mw.HEADING + 180, 360) ELSE mw.HEADING END AS HEADING,
                ROUND(mw.EFFECTIVE_MILES / mw.ROUTE_MILES * 100, 1) AS PERCENT_COMPLETE,
                CASE WHEN mw.IS_RETURN_TRIP THEN mw.END_CITY || ' > ' || mw.START_CITY
                    ELSE mw.START_CITY || ' > ' || mw.END_CITY END AS ROUTE_DISPLAY,
                mw.ROUTE_NAME,
                CASE WHEN mw.IS_RETURN_TRIP THEN mw.END_CITY ELSE mw.START_CITY END AS START_CITY,
                CASE WHEN mw.IS_RETURN_TRIP THEN mw.START_CITY ELSE mw.END_CITY END AS END_CITY,
                mw.ROUTE_MILES,
                ROUND(mw.EFFECTIVE_MILES, 0) AS MILES_DRIVEN,
                ROUND(mw.ROUTE_MILES - mw.MILES_THIS_LEG, 1) AS MILES_TO_DESTINATION,
                mw.IS_RETURN_TRIP, mw.LEGS_COMPLETED, mw.IS_BROKEN_DOWN,
                CASE
                    WHEN mw.MILES_THIS_LEG / NULLIF(mw.ROUTE_MILES, 0) > 0.98 THEN 'ARRIVING'
                    WHEN mw.MILES_THIS_LEG / NULLIF(mw.ROUTE_MILES, 0) < 0.02 AND mw.LEGS_COMPLETED > 0 THEN 'JUST_TURNED'
                    WHEN mw.IS_RETURN_TRIP THEN 'RETURNING'
                    ELSE 'OUTBOUND'
                END AS TRIP_PHASE,
                COALESCE(p.PREDICTED_FAILURE_TYPE, 'NORMAL') AS FAILURE_TYPE,
                CASE WHEN p.PREDICTED_HOURS_TO_FAILURE < 0 THEN NULL ELSE ROUND(p.PREDICTED_HOURS_TO_FAILURE, 1) END AS TTF_HOURS
            FROM matched_waypoints mw
            LEFT JOIN predictions p ON mw.ENTITY_ID = p.ENTITY_ID
            WHERE mw.rn = 1
        """)

        if not df.empty:
            import numpy as np
            df = df.replace([np.nan, np.inf, -np.inf], None)
            df = df.where(pd.notnull(df), None)

        trucks = df.to_dict(orient='records') if not df.empty else []
        result = {"trucks": trucks, "epoch": current_epoch}
        with cache_lock:
            map_trucks_cache["data"] = result
            map_trucks_cache["timestamp"] = current_time
            map_trucks_cache["epoch"] = current_epoch
        return result
    except Exception as e:
        logger.error(f"Map trucks error: {e}")
        return {"trucks": [], "error": str(e)}


@app.get("/api/map/routes")
def get_map_routes():
    global map_routes_cache
    current_time = time.time()
    with cache_lock:
        if (map_routes_cache["data"] is not None and
            (current_time - map_routes_cache["timestamp"]) < STATIC_CACHE_TTL):
            return map_routes_cache["data"]
    try:
        schema = SCHEMA_PREFIX or "NEW_FTFP.DATA"
        df = execute_query(f"""
            SELECT ENTITY_ID, ROUTE_NAME, START_CITY, END_CITY, GEOMETRY, POINT_COUNT
            FROM {schema}.ROUTE_GEOMETRY_DETAILED ORDER BY ENTITY_ID
        """)
        routes = []
        for _, row in df.iterrows():
            entity_id = row.get("ENTITY_ID")
            geom = row.get("GEOMETRY")
            if isinstance(geom, str):
                geom = json.loads(geom)
            coords = geom.get("coordinates", []) if geom else []
            import math
            total_dist = 0
            for i in range(1, len(coords)):
                lon1, lat1 = coords[i-1]
                lon2, lat2 = coords[i]
                R = 3959
                dlat = math.radians(lat2 - lat1)
                dlon = math.radians(lon2 - lon1)
                a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                total_dist += R * c
            routes.append({
                "entity_id": entity_id, "route_name": row.get("ROUTE_NAME"),
                "geometry": geom, "distance_miles": round(total_dist, 1),
                "start_city": row.get("START_CITY"), "end_city": row.get("END_CITY"),
                "point_count": int(row.get("POINT_COUNT") or 0)
            })
        result = {"routes": routes}
        with cache_lock:
            map_routes_cache["data"] = result
            map_routes_cache["timestamp"] = current_time
        return result
    except Exception as e:
        logger.error(f"Map routes error: {e}")
        return {"routes": [], "error": str(e)}


@app.get("/api/map/services")
def get_map_services():
    global map_services_cache
    current_time = time.time()
    with cache_lock:
        if (map_services_cache["data"] is not None and
            (current_time - map_services_cache["timestamp"]) < STATIC_CACHE_TTL):
            return map_services_cache["data"]
    try:
        schema = SCHEMA_PREFIX or "NEW_FTFP.DATA"
        df = execute_query(f"SELECT CENTER_ID, NAME AS CENTER_NAME, CITY, STATE_CODE AS STATE, LATITUDE, LONGITUDE FROM {schema}.SERVICE_CENTERS")
        centers = df.to_dict(orient='records') if not df.empty else []
        result = {"centers": centers}
        node_df = execute_query(f"SELECT NODE_ID, LONGITUDE, LATITUDE FROM {schema}.FREIGHT_NODES")
        if not node_df.empty:
            nodes = {}
            for _, nr in node_df.iterrows():
                nodes[nr['NODE_ID']] = [float(nr['LONGITUDE']), float(nr['LATITUDE'])]
            result["freight_nodes"] = nodes
        with cache_lock:
            map_services_cache["data"] = result
            map_services_cache["timestamp"] = current_time
        return result
    except Exception as e:
        return {"centers": [], "error": str(e)}


freight_graph = {}
freight_node_coords = {}
freight_graph_loaded = False

routing_preloaded = False
all_truck_route_nodes = {}
all_truck_route_miles = {}
all_truck_waypoints = {}
all_detour_routes = {}
all_geom_lookup_global = {}
all_service_centers = {}

def preload_routing_data(schema):
    global routing_preloaded, all_truck_route_nodes, all_truck_route_miles
    global all_detour_routes, all_geom_lookup_global, all_service_centers, all_truck_waypoints
    if routing_preloaded:
        return
    try:
        logger.info("Preloading routing data...")

        trn_df = execute_query(f"""
            WITH node_positions AS (
                SELECT trn.ENTITY_ID, trn.NODE_ID, trn.SEQUENCE_NUM,
                    fn.LATITUDE AS NODE_LAT, fn.LONGITUDE AS NODE_LNG
                FROM {schema}.TRUCK_ROUTE_NODES trn
                JOIN {schema}.FREIGHT_NODES fn ON trn.NODE_ID = fn.NODE_ID
            ),
            waypoint_match AS (
                SELECT np.ENTITY_ID, np.NODE_ID, np.SEQUENCE_NUM, w.MILES_FROM_START,
                    ROW_NUMBER() OVER (PARTITION BY np.ENTITY_ID, np.NODE_ID
                        ORDER BY SQRT(POW(np.NODE_LAT - w.LATITUDE, 2) + POW(np.NODE_LNG - w.LONGITUDE, 2))) AS rn
                FROM node_positions np
                JOIN {schema}.ROUTE_WAYPOINTS w ON w.ENTITY_ID = np.ENTITY_ID
            )
            SELECT ENTITY_ID, NODE_ID, SEQUENCE_NUM, MILES_FROM_START AS ROAD_MILES
            FROM waypoint_match WHERE rn = 1 ORDER BY ENTITY_ID, SEQUENCE_NUM
        """)
        for _, row in trn_df.iterrows():
            eid = row['ENTITY_ID']
            if eid not in all_truck_route_nodes:
                all_truck_route_nodes[eid] = []
            all_truck_route_nodes[eid].append({
                'NODE_ID': row['NODE_ID'],
                'SEQUENCE_NUM': int(row['SEQUENCE_NUM']),
                'ROAD_MILES': float(row['ROAD_MILES'])
            })

        rm_df = execute_query(f"""
            SELECT ENTITY_ID, MAX(MILES_FROM_START) AS ROUTE_MILES
            FROM {schema}.ROUTE_WAYPOINTS GROUP BY ENTITY_ID
        """)
        for _, row in rm_df.iterrows():
            all_truck_route_miles[row['ENTITY_ID']] = float(row['ROUTE_MILES'])

        wp_df = execute_query(f"""
            SELECT ENTITY_ID, MILES_FROM_START, LATITUDE, LONGITUDE
            FROM {schema}.ROUTE_WAYPOINTS
            ORDER BY ENTITY_ID, MILES_FROM_START
        """)
        for _, row in wp_df.iterrows():
            eid = row['ENTITY_ID']
            if eid not in all_truck_waypoints:
                all_truck_waypoints[eid] = []
            all_truck_waypoints[eid].append((float(row['MILES_FROM_START']), float(row['LONGITUDE']), float(row['LATITUDE'])))

        dr_df = execute_query(f"""
            SELECT dr.FROM_NODE_ID, dr.TO_CENTER_ID, dr.TO_CENTER_NODE_ID,
                dr.DISTANCE_MILES, dr.DURATION_HOURS, dr.PATH_NODES,
                sc.NAME, sc.CITY, sc.STATE_CODE, sc.LATITUDE AS SC_LAT, sc.LONGITUDE AS SC_LNG
            FROM {schema}.DETOUR_ROUTES dr
            JOIN {schema}.SERVICE_CENTERS sc ON dr.TO_CENTER_ID = sc.CENTER_ID
            WHERE dr.DISTANCE_MILES >= 0
            ORDER BY dr.DISTANCE_MILES ASC
        """)
        for _, row in dr_df.iterrows():
            key = row['FROM_NODE_ID']
            if key not in all_detour_routes:
                all_detour_routes[key] = []
            all_detour_routes[key].append({
                'FROM_NODE_ID': row['FROM_NODE_ID'],
                'TO_CENTER_ID': int(row['TO_CENTER_ID']),
                'TO_CENTER_NODE_ID': row['TO_CENTER_NODE_ID'],
                'DISTANCE_MILES': float(row['DISTANCE_MILES']),
                'DURATION_HOURS': float(row['DURATION_HOURS']),
                'PATH_NODES': row['PATH_NODES'],
                'NAME': row['NAME'],
                'CITY': row['CITY'],
                'STATE_CODE': row['STATE_CODE'],
                'SC_LAT': float(row['SC_LAT']),
                'SC_LNG': float(row['SC_LNG']),
            })

        geom_df = execute_query(f"SELECT FROM_NODE_ID, TO_CENTER_NODE_ID, GEOMETRY_JSON FROM {schema}.DETOUR_ROUTES WHERE GEOMETRY_JSON IS NOT NULL")
        for _, gr in geom_df.iterrows():
            k = f"{gr['FROM_NODE_ID']}:{gr['TO_CENTER_NODE_ID']}"
            all_geom_lookup_global[k] = gr['GEOMETRY_JSON']

        sc_df = execute_query(f"SELECT CENTER_ID, NAME, NODE_ID, LATITUDE, LONGITUDE FROM {schema}.SERVICE_CENTERS")
        for _, row in sc_df.iterrows():
            all_service_centers[int(row['CENTER_ID'])] = {
                'NODE_ID': row['NODE_ID'],
                'LAT': float(row['LATITUDE']),
                'LNG': float(row['LONGITUDE']),
            }

        routing_preloaded = True
        logger.info(f"Routing preloaded: {len(all_truck_route_nodes)} trucks, {len(all_detour_routes)} departure nodes, {len(all_geom_lookup_global)} geometries, {sum(len(v) for v in all_truck_waypoints.values())} waypoints")
    except Exception as e:
        logger.error(f"Failed to preload routing data: {e}")
        import traceback
        traceback.print_exc()

def load_freight_graph(schema):
    global freight_graph, freight_node_coords, freight_graph_loaded
    if freight_graph_loaded:
        return
    try:
        df = execute_query(f"SELECT FROM_NODE, TO_NODE, MILES FROM {schema}.ADJACENCY_LIST")
        freight_graph = {}
        for _, row in df.iterrows():
            freight_graph.setdefault(row['FROM_NODE'], []).append((row['TO_NODE'], float(row['MILES'])))
        ndf = execute_query(f"SELECT NODE_ID, LONGITUDE, LATITUDE FROM {schema}.FREIGHT_NODES")
        freight_node_coords = {}
        for _, row in ndf.iterrows():
            freight_node_coords[row['NODE_ID']] = [float(row['LONGITUDE']), float(row['LATITUDE'])]
        freight_graph_loaded = True
        logger.info(f"Freight graph loaded: {len(freight_graph)} nodes, {len(freight_node_coords)} coords")
    except Exception as e:
        logger.warning(f"Failed to load freight graph: {e}")

def dijkstra_shortest(from_node, to_nodes):
    if from_node in to_nodes:
        return (0, [from_node], from_node)
    dist = {from_node: 0}
    prev = {from_node: None}
    heap = [(0, from_node)]
    while heap:
        d, u = heapq.heappop(heap)
        if u in to_nodes:
            path = []
            n = u
            while n is not None:
                path.append(n)
                n = prev[n]
            return (d, list(reversed(path)), u)
        if d > dist.get(u, float('inf')):
            continue
        for v, w in freight_graph.get(u, []):
            nd = d + w
            if nd < dist.get(v, float('inf')):
                dist[v] = nd
                prev[v] = u
                heapq.heappush(heap, (nd, v))
    return (float('inf'), [], None)


class LRUCache(OrderedDict):
    def __init__(self, maxsize=100):
        super().__init__()
        self.maxsize = maxsize
    def get(self, key, default=None):
        if key in self:
            self.move_to_end(key)
            return self[key]
        return default
    def set(self, key, value):
        if key in self:
            self.move_to_end(key)
        self[key] = value
        if len(self) > self.maxsize:
            self.popitem(last=False)

service_rec_cache = LRUCache(maxsize=500)

def get_current_epoch():
    now = time.time()
    with cache_lock:
        if epoch_cache["epoch"] > 0 and (now - epoch_cache["timestamp"]) < EPOCH_CACHE_TTL:
            return epoch_cache["epoch"]
    try:
        schema = SCHEMA_PREFIX or "NEW_FTFP.DATA"
        df = execute_query(f"SELECT NEXT_EPOCH FROM {schema}.STREAM_STATE WHERE STREAM_NAME = 'NORMAL_TO_TELEMETRY'")
        if not df.empty:
            ep = int(df.iloc[0]['NEXT_EPOCH'])
            with cache_lock:
                epoch_cache["epoch"] = ep
                epoch_cache["timestamp"] = now
            return ep
    except:
        pass
    with cache_lock:
        return epoch_cache["epoch"]


@app.get("/api/map/service-recommendation/{entity_id}")
def get_service_recommendation(entity_id: str, ttf_hours: float = 6.0, truck_lat: float = None, truck_lng: float = None, include_ai_explanation: bool = False, failure_type: str = "PREDICTED_FAILURE"):
    try:
        schema = SCHEMA_PREFIX or "NEW_FTFP.DATA"
        if truck_lat is None or truck_lng is None:
            truck_lat, truck_lng = 35.0, -100.0

        if not routing_preloaded:
            load_freight_graph(schema)
            preload_routing_data(schema)

        current_epoch = get_current_epoch()
        cache_key = f"{entity_id}:{current_epoch}:{round(ttf_hours*2)/2}"
        cached = service_rec_cache.get(cache_key)
        if cached is not None:
            return cached

        max_safe_miles = ttf_hours * SAFETY_FACTOR * SPEED_MPH

        route_nodes_list = all_truck_route_nodes.get(entity_id, [])
        if not route_nodes_list:
            return {"error": "Truck not found in routing data", "entity_id": entity_id}

        route_node_set = set()
        route_node_seq = {}
        route_node_miles = {}
        for rn in route_nodes_list:
            nid = rn['NODE_ID']
            route_node_set.add(nid)
            route_node_seq[nid] = rn['SEQUENCE_NUM']
            route_node_miles[nid] = rn['ROAD_MILES']

        nearest_node = None
        nearest_dist = float('inf')
        for nid in route_node_set:
            if nid in freight_node_coords:
                c = freight_node_coords[nid]
                d = (truck_lat - c[1])**2 + (truck_lng - c[0])**2
                if d < nearest_dist:
                    nearest_dist = d
                    nearest_node = nid

        if not nearest_node:
            return {"error": "Cannot find nearest node", "entity_id": entity_id}

        route_miles = all_truck_route_miles.get(entity_id, 0)
        is_return = False
        truck_cum_miles = route_node_miles.get(nearest_node, 0)
        if route_miles > 0 and current_epoch > 0:
            total_driven = current_epoch * MILES_PER_EPOCH
            legs = int(total_driven // route_miles) if route_miles > 0 else 0
            is_return = legs % 2 == 1
            miles_this_leg = total_driven % route_miles if route_miles > 0 else 0
            truck_cum_miles = (route_miles - miles_this_leg) if is_return else miles_this_leg

        bracket_behind = None
        bracket_ahead = None
        sorted_nodes = sorted(route_node_miles.items(), key=lambda x: x[1])
        for nid, rm in sorted_nodes:
            if rm <= truck_cum_miles:
                bracket_behind = nid
            if rm > truck_cum_miles and bracket_ahead is None:
                bracket_ahead = nid
        if is_return:
            bracket_behind, bracket_ahead = bracket_ahead, bracket_behind

        departure_nodes = set()
        if bracket_behind:
            departure_nodes.add(bracket_behind)
        if bracket_ahead:
            departure_nodes.add(bracket_ahead)
        if nearest_node not in departure_nodes:
            departure_nodes.add(nearest_node)

        detour_rows = []
        for dn in departure_nodes:
            if dn in all_detour_routes:
                detour_rows.extend(all_detour_routes[dn])

        if not detour_rows:
            return {"error": "No pre-computed routes found", "entity_id": entity_id, "nearest_node": nearest_node}

        reconnect_cache_local = {}
        def get_best_reconnection(center_node_id, dist_to_center):
            rck = f"{center_node_id}:{dist_to_center:.1f}"
            if rck in reconnect_cache_local:
                return reconnect_cache_local[rck]
            ahead_candidates = []
            for nid, rm in route_node_miles.items():
                if is_return:
                    if rm < truck_cum_miles:
                        ahead_candidates.append((abs(truck_cum_miles - rm), nid))
                else:
                    if rm > truck_cum_miles:
                        ahead_candidates.append((rm - truck_cum_miles, nid))
            ahead_candidates.sort(key=lambda x: x[0])
            ahead_nodes = set(nid for _, nid in ahead_candidates[:5])
            if not ahead_nodes:
                reconnect_cache_local[rck] = (0, [], None)
                return reconnect_cache_local[rck]
            dist_map = {center_node_id: 0}
            prev_map = {center_node_id: None}
            heap = [(0, center_node_id)]
            best_added = float('inf')
            best_result = (float('inf'), [], None)
            while heap:
                d, u = heapq.heappop(heap)
                if d > dist_map.get(u, float('inf')):
                    continue
                if u in ahead_nodes:
                    rc_road_miles = route_node_miles[u]
                    route_seg = abs(rc_road_miles - truck_cum_miles)
                    added = max(0, dist_to_center + d - route_seg)
                    if added < best_added:
                        best_added = added
                        path = []
                        n = u
                        while n is not None:
                            path.append(n)
                            n = prev_map[n]
                        best_result = (d, list(reversed(path)), u)
                for v, w in freight_graph.get(u, []):
                    nd = d + w
                    if nd < dist_map.get(v, float('inf')):
                        dist_map[v] = nd
                        prev_map[v] = u
                        heapq.heappush(heap, (nd, v))
            reconnect_cache_local[rck] = best_result
            return best_result

        truck_pos_coord = [truck_lng, truck_lat]

        best_by_center = {}
        for row in detour_rows:
            from_node = row['FROM_NODE_ID']
            dist_from_node = row['DISTANCE_MILES']
            dur_from_node = row['DURATION_HOURS']
            center_node = row['TO_CENTER_NODE_ID']
            center_id = row['TO_CENTER_ID']
            on_route = center_node in route_node_set

            geom_key = f"{from_node}:{center_node}"
            geom_raw = all_geom_lookup_global.get(geom_key)
            geom = None
            if geom_raw:
                try:
                    geom = json.loads(geom_raw) if isinstance(geom_raw, str) else geom_raw
                except:
                    pass

            from_node_miles = route_node_miles.get(from_node, truck_cum_miles)
            dist_truck_to_departure = abs(truck_cum_miles - from_node_miles)

            ahead = False
            detour_miles = 0
            effective_dist = 0
            reconnect_node = None
            reconnect_dist = 0
            reconnect_path = []
            reconnect_geom = None

            if on_route:
                center_road_miles = route_node_miles.get(center_node, 0)
                if is_return:
                    ahead = center_road_miles < truck_cum_miles
                else:
                    ahead = center_road_miles > truck_cum_miles
                effective_dist = abs(center_road_miles - truck_cum_miles)
                if ahead:
                    detour_miles = 0
                else:
                    detour_miles = effective_dist * 2

                waypoints = all_truck_waypoints.get(entity_id, [])
                if waypoints:
                    from_m = min(truck_cum_miles, center_road_miles)
                    to_m = max(truck_cum_miles, center_road_miles)
                    seg = [(lng, lat) for m, lng, lat in waypoints if from_m - 1 <= m <= to_m + 1]
                    if len(seg) > 1:
                        if (is_return and ahead) or (not is_return and not ahead):
                            seg = list(reversed(seg))
                        geom = {"type": "LineString", "coordinates": [truck_pos_coord] + seg}
                    else:
                        center_coord = freight_node_coords.get(center_node)
                        if center_coord:
                            geom = {"type": "LineString", "coordinates": [truck_pos_coord, center_coord]}
            else:
                dist_to_center = dist_truck_to_departure + dist_from_node
                effective_dist = dist_to_center
                rc_dist, rc_path, rc_node = get_best_reconnection(center_node, dist_to_center)
                if rc_node:
                    reconnect_node = rc_node
                    reconnect_dist = rc_dist
                    reconnect_path = rc_path
                    reconnect_road_miles = route_node_miles[rc_node]
                    route_segment = abs(reconnect_road_miles - truck_cum_miles)
                    detour_miles = max(0, dist_to_center + rc_dist - route_segment)
                    fwd_key = f"{center_node}:{rc_node}"
                    rev_key = f"{rc_node}:{center_node}"
                    if fwd_key in all_geom_lookup_global:
                        g = all_geom_lookup_global[fwd_key]
                        try:
                            reconnect_geom = json.loads(g) if isinstance(g, str) else g
                        except:
                            pass
                    if not reconnect_geom and rev_key in all_geom_lookup_global:
                        g2 = all_geom_lookup_global[rev_key]
                        try:
                            rg = json.loads(g2) if isinstance(g2, str) else g2
                            if rg and rg.get('coordinates'):
                                reconnect_geom = {"type": "LineString", "coordinates": list(reversed(rg['coordinates']))}
                        except:
                            pass
                    if not reconnect_geom:
                        rc_coords = []
                        for rn in rc_path:
                            if rn in freight_node_coords:
                                rc_coords.append(freight_node_coords[rn])
                        if len(rc_coords) > 1:
                            reconnect_geom = {"type": "LineString", "coordinates": rc_coords}
                else:
                    detour_miles = dist_to_center * 2

            if not on_route and geom and geom.get('coordinates') and len(geom['coordinates']) > 1:
                coords = geom['coordinates']
                geom = {"type": "LineString", "coordinates": [truck_pos_coord] + coords}

            effective_dur = effective_dist / SPEED_MPH if SPEED_MPH > 0 else dur_from_node
            can_reach = effective_dist <= max_safe_miles

            if on_route and ahead and can_reach:
                score = effective_dist
            elif can_reach:
                score = 10000 + detour_miles
            else:
                score = 20000 + effective_dist

            via_type = "ON_ROUTE_AHEAD" if (on_route and ahead) else ("ON_ROUTE_BEHIND" if on_route else "DETOUR")

            if center_id not in best_by_center or score < best_by_center[center_id]['_score']:
                best_by_center[center_id] = {
                    "center_id": center_id,
                    "name": row['NAME'],
                    "city": row['CITY'],
                    "state": row['STATE_CODE'],
                    "lat": row['SC_LAT'],
                    "lng": row['SC_LNG'],
                    "latitude": row['SC_LAT'],
                    "longitude": row['SC_LNG'],
                    "distance_mi": round(effective_dist),
                    "distance_miles": round(effective_dist),
                    "drive_hrs": round(effective_dur, 1),
                    "duration_hours": round(effective_dur, 1),
                    "on_route": on_route,
                    "ahead": ahead,
                    "safe": can_reach,
                    "can_reach_safely": can_reach,
                    "node_id": center_node,
                    "departure_node": from_node,
                    "detour_mi": round(detour_miles, 1),
                    "total_added_miles": round(detour_miles, 1),
                    "reconnect_node": reconnect_node,
                    "reconnect_dist": round(reconnect_dist, 1) if reconnect_dist else 0,
                    "reconnect_path": reconnect_path,
                    "reconnect_geometry": reconnect_geom,
                    "route_geometry": geom,
                    "via": via_type,
                    "_score": score,
                }

        options = list(best_by_center.values())
        options.sort(key=lambda o: o['_score'])
        for i, opt in enumerate(options):
            opt['rank'] = i + 1
            opt.pop('_score', None)
        recommended = options[0] if options else None

        reasoning = ""
        if recommended:
            if recommended['safe'] and recommended.get('ahead') and recommended['on_route']:
                reasoning = f"{recommended['name']} is on your route ahead — {recommended['distance_mi']} mi ({recommended['drive_hrs']:.1f} hrs), zero detour. Best option!"
            elif recommended['safe'] and recommended['on_route']:
                reasoning = f"{recommended['name']} is on your route behind — {recommended['distance_mi']} mi ({recommended['drive_hrs']:.1f} hrs), {recommended['detour_mi']} mi added backtrack."
            elif recommended['safe'] and recommended.get('reconnect_node'):
                reasoning = f"{recommended['name']} is off-route — {recommended['distance_mi']} mi ({recommended['drive_hrs']:.1f} hrs), reconnects at {recommended['reconnect_node']}. {recommended['total_added_miles']} mi total added."
            elif recommended['safe']:
                reasoning = f"{recommended['name']} is {recommended['distance_mi']} mi away ({recommended['drive_hrs']:.1f} hrs). Reachable safely. {recommended['detour_mi']} mi detour."
            else:
                reasoning = f"EMERGENCY: Closest is {recommended['name']} at {recommended['distance_mi']} mi. TTF too short to reach any center safely!"

        result = {
            "entity_id": entity_id,
            "ttf_hours": ttf_hours,
            "max_safe_miles": round(max_safe_miles),
            "truck_position_miles": round(truck_cum_miles, 1),
            "recommended": recommended,
            "optimal": recommended,
            "options": options[:10],
            "reasoning": reasoning,
            "reason": reasoning,
            "nearest_node": nearest_node,
            "truck_lat": truck_lat,
            "truck_lng": truck_lng,
            "routing_backend": "v5.3.8-zero-sql"
        }

        if include_ai_explanation and recommended:
            try:
                ai_df = execute_query(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b',
                        'You are a fleet dispatch AI. Explain why truck {entity_id} with {failure_type} failure ({ttf_hours:.1f}h TTF) should go to {recommended["name"]} in {recommended["city"]} ({recommended["distance_mi"]} mi, {recommended["drive_hrs"]:.1f}h drive). Be concise (2-3 sentences).'
                    ) as explanation
                """)
                if not ai_df.empty:
                    explanation = ai_df.iloc[0]['EXPLANATION']
                    if isinstance(explanation, str):
                        explanation = explanation.strip('"')
                    result['ai_explanation'] = explanation
            except Exception as ex:
                logger.warning(f"Cortex explanation failed: {ex}")

        service_rec_cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Service recommendation error: {e}")
        return {"error": str(e), "entity_id": entity_id}


@app.get("/api/map/detour-route/{entity_id}/{center_id}")
def get_detour_route(entity_id: str, center_id: int):
    try:
        schema = SCHEMA_PREFIX or "NEW_FTFP.DATA"

        truck_node_df = execute_query(f"""
            WITH route_nodes AS (
                SELECT trn.NODE_ID, fn.LATITUDE, fn.LONGITUDE
                FROM {schema}.TRUCK_ROUTE_NODES trn
                JOIN {schema}.FREIGHT_NODES fn ON trn.NODE_ID = fn.NODE_ID
                WHERE trn.ENTITY_ID = '{entity_id}'
            ),
            truck_pos AS (
                SELECT LATITUDE, LONGITUDE FROM (
                    SELECT w.LATITUDE, w.LONGITUDE, ROW_NUMBER() OVER (ORDER BY ABS(w.MILES_FROM_START - (
                        SELECT NEXT_EPOCH * {MILES_PER_EPOCH} FROM {schema}.STREAM_STATE WHERE STREAM_NAME = '{STREAM_NAME}'
                    ))) AS rn
                    FROM {schema}.ROUTE_WAYPOINTS w WHERE w.ENTITY_ID = '{entity_id}'
                ) WHERE rn = 1
            )
            SELECT rn.NODE_ID, SQRT(POW(tp.LATITUDE - rn.LATITUDE, 2) + POW(tp.LONGITUDE - rn.LONGITUDE, 2)) AS DIST
            FROM route_nodes rn, truck_pos tp
            ORDER BY DIST ASC LIMIT 1
        """)

        if truck_node_df.empty:
            return {"error": "Could not determine truck node"}

        nearest_node = truck_node_df.iloc[0]['NODE_ID']

        geom_df = execute_query(f"""
            SELECT GEOMETRY_JSON, DISTANCE_MILES, DURATION_HOURS, PATH_NODES
            FROM {schema}.DETOUR_ROUTES
            WHERE FROM_NODE_ID = '{nearest_node}' AND TO_CENTER_ID = {center_id}
        """)

        if geom_df.empty:
            return {"error": "No pre-computed route found"}

        row = geom_df.iloc[0]
        geom = row['GEOMETRY_JSON']
        if isinstance(geom, str) and geom:
            try:
                geom = json.loads(geom)
            except:
                geom = None

        return {
            "geometry": geom,
            "distance_miles": float(row['DISTANCE_MILES']),
            "duration_hours": float(row['DURATION_HOURS']),
            "path_nodes": row['PATH_NODES'],
            "from_node": nearest_node,
            "to_center_id": center_id
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/ai/capabilities")
def get_ai_capabilities():
    return {
        "routing": {
            "backend": "Pre-computed Dijkstra (1081 routes)",
            "speed_mph": SPEED_MPH,
            "safety_factor": SAFETY_FACTOR,
            "description": "Routes pre-computed through 75-edge freight network with real road geometry"
        },
        "machine_learning": {
            "CLASSIFY_FAILURE": "XGBoost classifier for failure type prediction",
            "PREDICT_TTF": "XGBoost regressor for time-to-failure estimation",
            "PREDICT_TTF_TEMPORAL": "Time-decay adjusted TTF model (16 features)"
        },
        "cortex_ai": {
            "model": "llama3.1-8b",
            "description": "Natural language explanations for routing decisions"
        }
    }


active_connections: List[WebSocket] = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await asyncio.sleep(5)
            telemetry_df = execute_query(f"""
                WITH latest AS (SELECT MAX(timestamp) AS ts FROM {TELEMETRY})
                SELECT * FROM {TELEMETRY} WHERE timestamp = (SELECT ts FROM latest) ORDER BY entity_id
            """)
            await websocket.send_json({
                "type": "telemetry_update",
                "data": json.loads(telemetry_df.to_json(orient='records', date_format='iso'))
            })
    except WebSocketDisconnect:
        active_connections.remove(websocket)


frontend_build = Path(__file__).parent / "frontend" / "build"
logger.info(f"Looking for frontend at: {frontend_build}")
if frontend_build.exists():
    logger.info(f"Frontend found, mounting static files")
    app.mount("/", StaticFiles(directory=str(frontend_build), html=True), name="static")
else:
    logger.warning(f"Frontend not found at {frontend_build}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

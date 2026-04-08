#!/bin/bash
set -e

echo "================================================================="
echo "  Fleet Telemetry Failure Prediction (FTFP) - Setup"
echo "  Deploys the full FTFP application on a Snowflake account"
echo "================================================================="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# -------------------------------------------------------------------------
# Prerequisites
# -------------------------------------------------------------------------
check_prereqs() {
    echo -e "${BOLD}Checking prerequisites...${NC}"
    local missing=0
    for cmd in snow docker python3 openssl; do
        if ! command -v "$cmd" &>/dev/null; then
            echo -e "  ${RED}✗ $cmd not found${NC}"
            missing=1
        else
            echo -e "  ${GREEN}✓ $cmd${NC}"
        fi
    done
    docker info &>/dev/null 2>&1 || { echo -e "  ${RED}✗ Docker daemon not running${NC}"; missing=1; }
    if [ $missing -eq 1 ]; then
        echo -e "\n${RED}Please install missing prerequisites and re-run.${NC}"
        exit 1
    fi
    echo ""
}

# -------------------------------------------------------------------------
# Connection setup
# -------------------------------------------------------------------------
setup_connection() {
    echo -e "${BOLD}Connection Setup${NC}"
    echo "You need a Snowflake CLI connection configured."
    echo "Available connections:"
    snow connection list 2>/dev/null || true
    echo ""
    read -p "Enter connection name to use: " CONNECTION_NAME
    echo ""

    echo "Testing connection..."
    snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_USER()" >/dev/null 2>&1 || {
        echo -e "${RED}Connection test failed. Check your connection config.${NC}"
        exit 1
    }
    echo -e "${GREEN}Connection OK${NC}"
    echo ""

    ACCOUNT_INFO=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCT" --format json 2>/dev/null)
    ACCOUNT_LOCATOR=$(echo "$ACCOUNT_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['ACCT'])")
    ACCOUNT_LOWER=$(echo "$ACCOUNT_LOCATOR" | tr '[:upper:]' '[:lower:]')
    SNOWFLAKE_HOST="${ACCOUNT_LOWER}.snowflakecomputing.com"
    REGISTRY_HOST="${ACCOUNT_LOWER}.registry.snowflakecomputing.com"
    SNOWFLAKE_USER=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT CURRENT_USER()" --format json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['CURRENT_USER()'])")

    echo -e "  Account:  ${CYAN}${ACCOUNT_LOCATOR}${NC}"
    echo -e "  Host:     ${CYAN}${SNOWFLAKE_HOST}${NC}"
    echo -e "  Registry: ${CYAN}${REGISTRY_HOST}${NC}"
    echo -e "  User:     ${CYAN}${SNOWFLAKE_USER}${NC}"
    echo ""

    PLATFORM=$(snow sql --connection "$CONNECTION_NAME" -q "SELECT SPLIT_PART(CURRENT_REGION(), '_', 1)" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0][list(d[0].keys())[0]])")
    if [[ "$PLATFORM" != *"AWS"* ]]; then
        echo -e "${YELLOW}⚠  Non-AWS region detected ($PLATFORM). Some features may require AWS.${NC}"
        read -p "Continue anyway? (y/n): " CONT
        if [ "$CONT" != "y" ]; then exit 1; fi
    fi
}

snow_sql() {
    if [ -n "${SNOW_WH:-}" ]; then
        snow sql --connection "$CONNECTION_NAME" --warehouse "$SNOW_WH" "$@"
    else
        snow sql --connection "$CONNECTION_NAME" "$@"
    fi
}

SNOW_WH=""

# -------------------------------------------------------------------------
# Step 1: Infrastructure (DDL)
# -------------------------------------------------------------------------
create_infrastructure() {
    echo -e "${BOLD}[1/7] Creating database, schemas, tables, views, and functions...${NC}"
    snow_sql -f "$SCRIPT_DIR/snowflake/setup.sql"
    SNOW_WH="NEW_FTFP_WH"
    echo -e "${GREEN}✓ Infrastructure created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 2: Seed data (from pre-exported CSVs)
# -------------------------------------------------------------------------
seed_data() {
    echo -e "${BOLD}[2/7] Loading seed data from static exports...${NC}"
    echo "  Reassembling split files..."
    if ls "$SCRIPT_DIR/data/detour_routes.csv.gz.part_"* &>/dev/null; then
        cat "$SCRIPT_DIR/data/detour_routes.csv.gz.part_"* > /tmp/detour_routes.csv.gz
        echo "    Reassembled detour_routes.csv.gz"
    fi

    echo "  Uploading CSV files to DATA_STAGE..."

    for f in "$SCRIPT_DIR"/data/*.csv "$SCRIPT_DIR"/data/*.csv.gz; do
        [ -f "$f" ] || continue
        local basename=$(basename "$f")
        [[ "$basename" == detour_routes.csv.gz.part_* ]] && continue
        echo "    Uploading $basename..."
        snow stage copy "$f" @NEW_FTFP.DATA.DATA_STAGE/seed/ --overwrite --database NEW_FTFP --schema DATA --connection "$CONNECTION_NAME"
    done

    if [ -f /tmp/detour_routes.csv.gz ]; then
        echo "    Uploading detour_routes.csv.gz (large, ~117MB)..."
        snow stage copy /tmp/detour_routes.csv.gz @NEW_FTFP.DATA.DATA_STAGE/seed/ --overwrite --database NEW_FTFP --schema DATA --connection "$CONNECTION_NAME"
    fi

    echo "  Loading tables from CSV..."
    local CSV_FORMAT="TYPE = CSV COMPRESSION = AUTO FIELD_OPTIONALLY_ENCLOSED_BY = '\"' PARSE_HEADER = TRUE FIELD_DELIMITER = ',' NULL_IF = ('', '\\\\N', '\"\\\\N\"')"
    local COPY_OPTS="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = 'ABORT_STATEMENT'"

    load_table() {
        local TABLE=$1
        local FILE=$2
        echo "    Loading $TABLE..."
        snow_sql -q "TRUNCATE TABLE IF EXISTS NEW_FTFP.DATA.$TABLE;"
        snow_sql -q "COPY INTO NEW_FTFP.DATA.$TABLE FROM @NEW_FTFP.DATA.DATA_STAGE/seed/$FILE FILE_FORMAT = ($CSV_FORMAT) $COPY_OPTS;"
    }

    echo "  Loading seed tables..."
    load_table NORMAL_SEED normal_seed.csv.gz
    load_table ENGINE_FAILURE_SEED engine_failure_seed.csv.gz
    load_table TRANSMISSION_FAILURE_SEED transmission_failure_seed.csv.gz
    load_table ELECTRICAL_FAILURE_SEED electrical_failure_seed.csv.gz

    echo "  Loading route & service center tables..."
    load_table TRUCK_ROUTES truck_routes.csv
    load_table ROUTE_WAYPOINTS route_waypoints.csv.gz
    load_table SERVICE_CENTERS service_centers.csv
    load_table SERVICE_CENTER_ROUTES service_center_routes.csv

    echo "  Loading graph tables..."
    load_table FREIGHT_NODES freight_nodes.csv
    load_table FREIGHT_EDGES freight_edges.csv.gz
    load_table TRUCK_ROUTE_NODES truck_route_nodes.csv

    echo "  Loading detour routes (large, may take a minute)..."
    load_table DETOUR_ROUTES detour_routes.csv.gz

    echo "  Loading failure config and markers..."
    load_table FAILURE_CONFIG failure_config.csv
    load_table FIRST_FAILURE_MARKERS first_failure_markers.csv

    echo "  Loading ML training data (2.9M rows, may take a minute)..."
    snow stage copy "$SCRIPT_DIR/data/training/" @NEW_FTFP.DATA.DATA_STAGE/training/ --recursive --overwrite --database NEW_FTFP --schema DATA --connection "$CONNECTION_NAME"
    snow_sql -q "TRUNCATE TABLE IF EXISTS NEW_FTFP.DATA.TRAINING_TBL;"
    snow_sql -q "COPY INTO NEW_FTFP.DATA.TRAINING_TBL FROM @NEW_FTFP.DATA.DATA_STAGE/training/ FILE_FORMAT = ($CSV_FORMAT) $COPY_OPTS;"

    echo "  Uploading ML models to ML_MODELS stage..."
    snow stage copy "$SCRIPT_DIR/models/" @NEW_FTFP.DATA.ML_MODELS/models/ --overwrite --database NEW_FTFP --schema DATA --connection "$CONNECTION_NAME"

    echo -e "${GREEN}✓ Seed data loaded${NC}\n"
}

# -------------------------------------------------------------------------
# Step 3: Create views and ML functions
# -------------------------------------------------------------------------
create_views_and_functions() {
    echo -e "${BOLD}[3/7] Creating views and ML UDFs...${NC}"
    snow_sql -f "$SCRIPT_DIR/snowflake/create_views.sql"
    snow_sql -f "$SCRIPT_DIR/snowflake/create_functions.sql"
    echo -e "${GREEN}✓ Views and functions created${NC}\n"
}

# -------------------------------------------------------------------------
# Step 4: Key-pair authentication with SAFE KEY MANAGEMENT
# -------------------------------------------------------------------------
generate_new_key() {
    echo ""
    echo "  Generating RSA key pair..."
    TEMP_DIR=$(mktemp -d)
    openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out "$TEMP_DIR/key.p8" 2>/dev/null
    openssl rsa -in "$TEMP_DIR/key.p8" -pubout -out "$TEMP_DIR/key.pub" 2>/dev/null
    PUBLIC_KEY=$(grep -v "BEGIN\|END" "$TEMP_DIR/key.pub" | tr -d '\n')

    snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET RSA_PUBLIC_KEY='${PUBLIC_KEY}';"
    echo -e "  ${GREEN}✓ Public key assigned to ${SNOWFLAKE_USER}${NC}"
    rm -rf "$TEMP_DIR"
}

generate_key_slot_2() {
    echo ""
    echo "  Generating RSA key pair for RSA_PUBLIC_KEY_2..."
    TEMP_DIR=$(mktemp -d)
    openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out "$TEMP_DIR/key.p8" 2>/dev/null
    openssl rsa -in "$TEMP_DIR/key.p8" -pubout -out "$TEMP_DIR/key.pub" 2>/dev/null
    PUBLIC_KEY=$(grep -v "BEGIN\|END" "$TEMP_DIR/key.pub" | tr -d '\n')

    snow_sql -q "ALTER USER ${SNOWFLAKE_USER} SET RSA_PUBLIC_KEY_2='${PUBLIC_KEY}';"
    echo -e "  ${GREEN}✓ Public key assigned to RSA_PUBLIC_KEY_2${NC}"
    rm -rf "$TEMP_DIR"
}

create_keys() {
    echo -e "${BOLD}[4/7] Setting up key-pair authentication...${NC}"
    echo ""

    echo "  Checking for existing RSA key on ${SNOWFLAKE_USER}..."
    EXISTING_KEY=$(snow_sql -q "DESCRIBE USER ${SNOWFLAKE_USER};" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for row in data:
        if row.get('property') == 'RSA_PUBLIC_KEY':
            val = row.get('value', '')
            if val and val != 'null' and len(val) > 10:
                print('EXISTS')
                break
except:
    pass
" 2>/dev/null || echo "")

    if [ "$EXISTING_KEY" = "EXISTS" ]; then
        echo ""
        echo -e "${YELLOW}  RSA_PUBLIC_KEY already exists for user ${SNOWFLAKE_USER}${NC}"
        echo "  Another SPCS application may be using this key."
        echo "  Overwriting it will break that application's authentication."
        echo ""
        echo "  Options:"
        echo "    1) Reuse existing key (no changes needed)"
        echo "    2) Use RSA_PUBLIC_KEY_2 (secondary slot - BOTH apps work)"
        echo "    3) Generate NEW key (WARNING: breaks other SPCS apps!)"
        echo ""
        read -p "  Choice [1/2/3] (default 1 - recommended): " KEY_CHOICE
        KEY_CHOICE=${KEY_CHOICE:-1}

        case $KEY_CHOICE in
            1)
                echo -e "  ${GREEN}✓ Reusing existing key${NC}"
                ;;
            2)
                generate_key_slot_2
                ;;
            3)
                echo ""
                echo -e "  ${RED}WARNING: This will invalidate any other SPCS apps using this user!${NC}"
                read -p "  Are you sure? (yes/no): " CONFIRM
                if [ "$CONFIRM" != "yes" ]; then
                    echo "  Aborted."
                    exit 1
                fi
                generate_new_key
                ;;
            *)
                echo -e "  ${GREEN}✓ Reusing existing key (default)${NC}"
                ;;
        esac
    else
        generate_new_key
    fi

    echo -e "${GREEN}✓ Key-pair authentication configured${NC}\n"
}

# -------------------------------------------------------------------------
# Step 5: Build and push Docker image
# -------------------------------------------------------------------------
build_and_push() {
    echo -e "${BOLD}[5/7] Building and pushing Docker image...${NC}"

    snow spcs image-registry login --connection "$CONNECTION_NAME"

    REPO_URL=$(snow_sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA NEW_FTFP.DATA;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    if row.get('name','').upper() == 'FTFP_REPO':
        print(row['repository_url'])
        break
")

    IMAGE_TAG="${REPO_URL}/ftfp_app:v5.4.4"
    echo "  Building image: ${IMAGE_TAG}"

    docker buildx build --platform linux/amd64 \
        -t "$IMAGE_TAG" \
        -f "$SCRIPT_DIR/Dockerfile" \
        "$SCRIPT_DIR" \
        --load

    echo "  Pushing image..."
    docker push "$IMAGE_TAG"
    echo -e "${GREEN}✓ Image pushed to Snowflake registry${NC}\n"
}

# -------------------------------------------------------------------------
# Step 6: Create compute pool + service
# -------------------------------------------------------------------------
deploy_service() {
    echo -e "${BOLD}[6/7] Deploying SPCS service...${NC}"

    snow_sql -q "CREATE COMPUTE POOL IF NOT EXISTS NEW_FTFP_POOL MIN_NODES = 1 MAX_NODES = 1 INSTANCE_FAMILY = CPU_X64_S AUTO_RESUME = TRUE AUTO_SUSPEND_SECS = 300;"

    REPO_URL=$(snow_sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA NEW_FTFP.DATA;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    if row.get('name','').upper() == 'FTFP_REPO':
        print(row['repository_url'])
        break
")
    IMAGE_PATH="${REPO_URL}/ftfp_app:v5.4.4"

    sed "s|__IMAGE_PATH__|${IMAGE_PATH}|g" \
        "$SCRIPT_DIR/ftfp_service.yaml.template" > /tmp/ftfp_service.yaml

    snow_sql -q "CREATE STAGE IF NOT EXISTS NEW_FTFP.DATA.SPECS;"
    snow stage copy /tmp/ftfp_service.yaml @NEW_FTFP.DATA.SPECS/ --overwrite --database NEW_FTFP --schema DATA --connection "$CONNECTION_NAME"

    snow_sql -q "DROP SERVICE IF EXISTS NEW_FTFP.DATA.FTFP_SERVICE;"
    snow_sql -q "CREATE SERVICE NEW_FTFP.DATA.FTFP_SERVICE IN COMPUTE POOL NEW_FTFP_POOL FROM @NEW_FTFP.DATA.SPECS SPECIFICATION_FILE = 'ftfp_service.yaml' MIN_INSTANCES = 1 MAX_INSTANCES = 1;"

    echo "  Waiting for service to start..."
    for i in $(seq 1 40); do
        STATUS=$(snow_sql -q "SELECT SYSTEM\$GET_SERVICE_STATUS('NEW_FTFP.DATA.FTFP_SERVICE')" --format json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    status_json = json.loads(data[0][list(data[0].keys())[0]])
    print(status_json[0].get('status', 'UNKNOWN'))
except:
    print('PENDING')
" 2>/dev/null || echo "PENDING")
        echo "  Status: $STATUS ($i/40)"
        if [ "$STATUS" = "READY" ]; then
            break
        fi
        sleep 15
    done
    echo -e "${GREEN}✓ Service deployed${NC}\n"
}

# -------------------------------------------------------------------------
# Step 7: Show results
# -------------------------------------------------------------------------
show_results() {
    echo -e "${BOLD}[7/7] Getting service endpoint...${NC}"
    ENDPOINT=$(snow_sql -q "SHOW ENDPOINTS IN SERVICE NEW_FTFP.DATA.FTFP_SERVICE;" --format json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data:
    url = row.get('ingress_url', '')
    if url:
        print(url)
        break
" 2>/dev/null || echo "(endpoint not yet available)")

    echo ""
    echo -e "${GREEN}=================================================================${NC}"
    echo -e "${GREEN}  Setup Complete!${NC}"
    echo -e "${GREEN}=================================================================${NC}"
    echo ""
    echo -e "  App URL:      ${CYAN}https://${ENDPOINT}${NC}"
    echo -e "  Account:      ${ACCOUNT_LOCATOR}"
    echo -e "  Database:     NEW_FTFP"
    echo -e "  Service:      NEW_FTFP.DATA.FTFP_SERVICE"
    echo -e "  Pool:         NEW_FTFP_POOL"
    echo -e "  Warehouse:    NEW_FTFP_WH"
    echo ""
    echo "  The app simulates a fleet of 10 trucks with real-time telemetry."
    echo "  Use the controls in the sidebar to inject failures, fast-forward"
    echo "  time, and watch ML predictions update in real time."
    echo ""
    echo "  To tear down: ./teardown.sh"
    echo -e "${GREEN}=================================================================${NC}"
}

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
main() {
    check_prereqs
    setup_connection
    create_infrastructure    # Step 1: DDL (database, tables, stages)
    seed_data                # Step 2: Load CSVs + ML models
    create_views_and_functions # Step 3: Views + UDFs
    create_keys              # Step 4: RSA key-pair (safe management)
    build_and_push           # Step 5: Docker build + push
    deploy_service           # Step 6: Compute pool + SPCS service
    show_results             # Step 7: Show endpoint

    echo ""
    echo "To reset the simulation state:"
    echo "  snow sql --connection $CONNECTION_NAME -q \"TRUNCATE TABLE NEW_FTFP.DATA.TELEMETRY; TRUNCATE TABLE NEW_FTFP.DATA.PREDICTION_CACHE; TRUNCATE TABLE NEW_FTFP.DATA.ACTIVE_FAILURES; TRUNCATE TABLE NEW_FTFP.DATA.FAILURE_CONFIG; TRUNCATE TABLE NEW_FTFP.DATA.FIRST_FAILURE_MARKERS;\""
}

main

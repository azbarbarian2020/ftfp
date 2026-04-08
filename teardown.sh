#!/bin/bash
set -e

echo "================================================================="
echo "  Fleet Telemetry Failure Prediction (FTFP) - Teardown"
echo "================================================================="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}WARNING: This will destroy FTFP demo objects including:${NC}"
echo "  - Service: NEW_FTFP.DATA.FTFP_SERVICE"
echo "  - Compute pool: NEW_FTFP_POOL"
echo "  - Database: NEW_FTFP (all tables, stages, etc.)"
echo "  - Warehouse: NEW_FTFP_WH"
echo ""
echo -e "${GREEN}NOTE: User RSA keys will NOT be removed.${NC}"
echo ""
read -p "Are you sure? (type 'yes' to confirm): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then echo "Cancelled."; exit 0; fi

echo ""
read -p "Enter Snowflake CLI connection name: " CONNECTION_NAME

snow_sql() {
    snow sql --connection "$CONNECTION_NAME" "$@"
}

echo ""
echo "Running teardown..."

echo "  Suspending service..."
snow_sql -q "ALTER SERVICE IF EXISTS NEW_FTFP.DATA.FTFP_SERVICE SUSPEND;" 2>/dev/null || true
sleep 5

echo "  Dropping service..."
snow_sql -q "DROP SERVICE IF EXISTS NEW_FTFP.DATA.FTFP_SERVICE;" 2>/dev/null || true

echo "  Dropping compute pool..."
snow_sql -q "DROP COMPUTE POOL IF EXISTS NEW_FTFP_POOL;" 2>/dev/null || true

echo "  Dropping database NEW_FTFP..."
snow_sql -q "DROP DATABASE IF EXISTS NEW_FTFP;" 2>/dev/null || true

echo "  Dropping warehouse..."
snow_sql -q "DROP WAREHOUSE IF EXISTS NEW_FTFP_WH;" 2>/dev/null || true

echo ""
echo -e "${GREEN}=================================================================${NC}"
echo -e "${GREEN}  Teardown Complete${NC}"
echo -e "${GREEN}=================================================================${NC}"
echo ""
echo "  All FTFP demo objects have been removed."
echo ""
echo -e "  ${YELLOW}NOTE: User RSA keys were NOT removed.${NC}"
echo "  To manually remove RSA keys if needed:"
echo "    ALTER USER <username> UNSET RSA_PUBLIC_KEY;"
echo "    ALTER USER <username> UNSET RSA_PUBLIC_KEY_2;"
echo ""

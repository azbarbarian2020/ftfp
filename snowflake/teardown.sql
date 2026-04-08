-- ============================================================================
-- FTFP Teardown SQL
-- ============================================================================
DROP SERVICE IF EXISTS NEW_FTFP.DATA.FTFP_SERVICE;
DROP COMPUTE POOL IF EXISTS NEW_FTFP_POOL;
DROP DATABASE IF EXISTS NEW_FTFP;
DROP WAREHOUSE IF EXISTS NEW_FTFP_WH;

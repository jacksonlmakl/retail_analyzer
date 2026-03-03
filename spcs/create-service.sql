-- ============================================================
-- Create the SPCS Service
-- Run after: setup.sql, deploy.sh, and uploading the spec YAML.
--
-- Prerequisites:
--   1. setup.sql executed successfully
--   2. All Docker images pushed via deploy.sh
--   3. Spec YAML uploaded to @SPCS_SPECS stage
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYZER;

-- ── Upload spec (if not done via deploy.sh) ─────────────────
-- Via SnowSQL:
--   PUT 'file:///path/to/retail_analyzer/spcs/retail-analyzer.yaml'
--       @RETAIL_ANALYZER.PUBLIC.SPCS_SPECS
--       AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- ── Create the service ──────────────────────────────────────
CREATE SERVICE IF NOT EXISTS RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC
    IN COMPUTE POOL SCRAPER_POOL
    FROM @RETAIL_ANALYZER.PUBLIC.SPCS_SPECS
    SPECIFICATION_FILE = 'retail-analyzer.yaml'
    EXTERNAL_ACCESS_INTEGRATIONS = (SCRAPER_EGRESS)
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- ── Grant endpoint access to roles ──────────────────────────
-- Required for PAT-based or programmatic access to public endpoints
GRANT SERVICE ROLE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC!ALL_ENDPOINTS_USAGE
    TO ROLE ACCOUNTADMIN;

GRANT SERVICE ROLE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC!ALL_ENDPOINTS_USAGE
    TO ROLE SYSADMIN;

-- ── Check status (wait ~2 min after CREATE) ─────────────────
SELECT SYSTEM$GET_SERVICE_STATUS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC');

-- ── Get public endpoint URLs ────────────────────────────────
-- Copy the ingress_url values into your .env file
SHOW ENDPOINTS IN SERVICE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC;

-- ════════════════════════════════════════════════════════════
-- DEBUGGING & MANAGEMENT
-- ════════════════════════════════════════════════════════════

-- View container logs (replace container name as needed):
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'redis', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'google-shopping-api', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'google-shopping-worker', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'grailed-worker', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'vestiaire-worker', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'rebag-worker', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'farfetch-worker', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'fashionphile-worker', 100);
-- SELECT SYSTEM$GET_SERVICE_LOGS('RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC', 0, 'secondstreet-worker', 100);

-- Suspend service (stops billing):
-- ALTER SERVICE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC SUSPEND;

-- Resume service:
-- ALTER SERVICE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC RESUME;

-- Redeploy after code changes (rebuild images, re-upload YAML, then):
-- ALTER SERVICE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC SUSPEND;
-- ALTER SERVICE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC
--     FROM @RETAIL_ANALYZER.PUBLIC.SPCS_SPECS
--     SPECIFICATION_FILE = 'retail-analyzer.yaml';
-- ALTER SERVICE RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC RESUME;

-- Drop everything:
-- DROP SERVICE IF EXISTS RETAIL_ANALYZER.PUBLIC.RETAIL_ANALYZER_SVC;
-- DROP COMPUTE POOL IF EXISTS SCRAPER_POOL;

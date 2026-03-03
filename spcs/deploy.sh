#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Build, tag, and push all Docker images to Snowflake SPCS,
# then upload the service spec YAML.
#
# Prerequisites:
#   1. Run spcs/setup.sql in Snowflake
#   2. Run:  SHOW IMAGE REPOSITORIES IN SCHEMA RETAIL_ANALYZER.IMAGES;
#      Copy the repository_url from the output.
#   3. Install Snowflake CLI:  pip install snowflake-cli
#   4. Configure a connection named "retail" in ~/.snowflake/config.toml:
#        [connections.retail]
#        account = "<ORG>-<ACCOUNT>"
#        user = "<your_user>"
#        authenticator = "SNOWFLAKE_JWT"
#        private_key_file = "/path/to/snowflake_rsa_key.p8"
#        role = "ACCOUNTADMIN"
#        warehouse = "COMPUTE_WH"
#        database = "RETAIL_ANALYZER"
#   5. Docker Desktop running with buildx support
#
# Usage:
#   export REGISTRY="orgname-acctname.registry.snowflakecomputing.com/retail_analyzer/images/repo"
#   bash spcs/deploy.sh
# ============================================================

REGISTRY="${REGISTRY:?Set REGISTRY to your Snowflake image repository URL (from SHOW IMAGE REPOSITORIES)}"
CONNECTION="${SNOW_CONNECTION:-retail}"

echo "=== Logging in to Snowflake Container Registry ==="
snow spcs image-registry login --connection "${CONNECTION}"

SERVICES=(
    "google-shopping:google_shopping/Dockerfile"
    "grailed:grailed/Dockerfile"
    "vestiaire:vestiaire/Dockerfile"
    "rebag:rebag/Dockerfile"
    "farfetch:farfetch/Dockerfile"
    "fashionphile:fashionphile/Dockerfile"
    "secondstreet:secondstreet/Dockerfile"
)

echo ""
echo "=== Building and pushing marketplace images (linux/amd64) ==="

for entry in "${SERVICES[@]}"; do
    NAME="${entry%%:*}"
    DOCKERFILE="${entry##*:}"
    TAG="${REGISTRY}/${NAME}:latest"

    echo ""
    echo "--- ${NAME} ---"
    echo "    Dockerfile: ${DOCKERFILE}"
    echo "    Tag:        ${TAG}"

    docker buildx build \
        --platform linux/amd64 \
        -f "${DOCKERFILE}" \
        -t "${TAG}" \
        --push \
        .
done

echo ""
echo "=== Building and pushing Redis image (linux/amd64) ==="
REDIS_TAG="${REGISTRY}/redis:7-alpine"
echo "FROM redis:7-alpine" | docker buildx build \
    --platform linux/amd64 \
    -t "${REDIS_TAG}" \
    --push \
    -

echo ""
echo "=== Uploading service spec to Snowflake stage ==="
snow sql --connection "${CONNECTION}" \
    -q "PUT 'file://$(pwd)/spcs/retail-analyzer.yaml' @RETAIL_ANALYZER.PUBLIC.SPCS_SPECS AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

echo ""
echo "=== Done! ==="
echo ""
echo "All images pushed and spec uploaded."
echo "Next: run spcs/create-service.sql in a Snowflake worksheet."

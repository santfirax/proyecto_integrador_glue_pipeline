#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${AWS_PROFILE_NAME:-}"
REGION="${AWS_REGION_NAME:-us-east-1}"
BUCKET="${S3_BUCKET_NAME:-eafit-proyecto-integrador-simem}"
ROLE_NAME="${GLUE_ROLE_NAME:-simem-glue-service-role}"
DATABASE_NAME="${GLUE_GOLD_DATABASE_NAME:-simem_gold}"
JOB_NAME="${GLUE_GOLD_JOB_NAME:-simem-silver-to-gold-features}"
CRAWLER_NAME="${GLUE_GOLD_CRAWLER_NAME:-simem-gold-features-crawler}"
SCRIPT_LOCAL_PATH="${SCRIPT_LOCAL_PATH:-${PROJECT_ROOT}/jobs/simem_silver_to_gold_features.py}"
SCRIPT_S3_PATH="s3://${BUCKET}/glue/scripts/simem_silver_to_gold_features.py"
SOURCE_PATH="s3://${BUCKET}/silver/simem-data/"
TARGET_PATH="s3://${BUCKET}/gold/simem-features/demanda-real-hourly/"
TRUST_POLICY_PATH="${PROJECT_ROOT}/infra/glue-trust-policy.json"
INLINE_POLICY_PATH="${PROJECT_ROOT}/infra/simem-glue-inline-policy.json"
CRAWLER_CONFIGURATION='{"Version":1.0}'

AWS_ARGS=(--region "${REGION}")

if [[ -n "${PROFILE}" ]]; then
  AWS_ARGS+=(--profile "${PROFILE}")
fi

echo "Subiendo script de Glue a ${SCRIPT_S3_PATH}"
aws s3 cp "${SCRIPT_LOCAL_PATH}" "${SCRIPT_S3_PATH}" "${AWS_ARGS[@]}"

if aws iam get-role --role-name "${ROLE_NAME}" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
  echo "El role ${ROLE_NAME} ya existe"
else
  echo "Creando role ${ROLE_NAME}"
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "file://${TRUST_POLICY_PATH}" \
    "${AWS_ARGS[@]}" \
    --output json >/dev/null
fi

echo "Adjuntando policy administrada AWSGlueServiceRole"
aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" \
  "${AWS_ARGS[@]}" >/dev/null || true

echo "Aplicando inline policy con acceso a S3"
aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "simem-glue-s3-access" \
  --policy-document "file://${INLINE_POLICY_PATH}" \
  "${AWS_ARGS[@]}" >/dev/null

ROLE_ARN="$(aws iam get-role --role-name "${ROLE_NAME}" "${AWS_ARGS[@]}" --query 'Role.Arn' --output text)"

if aws glue get-database --name "${DATABASE_NAME}" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
  echo "La database ${DATABASE_NAME} ya existe"
else
  echo "Creando database ${DATABASE_NAME}"
  aws glue create-database \
    --database-input "{\"Name\":\"${DATABASE_NAME}\",\"Description\":\"Gold layer de features SIMEM para modelado\"}" \
    "${AWS_ARGS[@]}" >/dev/null
fi

if aws glue get-job --job-name "${JOB_NAME}" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
  echo "Actualizando job ${JOB_NAME}"
  aws glue update-job \
    --job-name "${JOB_NAME}" \
    --job-update "{
      \"Role\":\"${ROLE_ARN}\",
      \"GlueVersion\":\"4.0\",
      \"ExecutionClass\":\"STANDARD\",
      \"Command\":{\"Name\":\"glueetl\",\"ScriptLocation\":\"${SCRIPT_S3_PATH}\",\"PythonVersion\":\"3\"},
      \"DefaultArguments\":{\"--SOURCE_PATH\":\"${SOURCE_PATH}\",\"--TARGET_PATH\":\"${TARGET_PATH}\",\"--job-language\":\"python\",\"--TARGET_DATASET_SLUG\":\"demanda-real\",\"--LABEL_HORIZON_HOURS\":\"24\",\"--DROP_INCOMPLETE_ROWS\":\"true\"},
      \"MaxRetries\":0,
      \"Timeout\":60,
      \"WorkerType\":\"G.1X\",
      \"NumberOfWorkers\":2
    }" \
    "${AWS_ARGS[@]}" >/dev/null
else
  echo "Creando job ${JOB_NAME}"
  aws glue create-job \
    --name "${JOB_NAME}" \
    --role "${ROLE_ARN}" \
    --glue-version "4.0" \
    --execution-class "STANDARD" \
    --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"${SCRIPT_S3_PATH}\",\"PythonVersion\":\"3\"}" \
    --default-arguments "{\"--SOURCE_PATH\":\"${SOURCE_PATH}\",\"--TARGET_PATH\":\"${TARGET_PATH}\",\"--job-language\":\"python\",\"--TARGET_DATASET_SLUG\":\"demanda-real\",\"--LABEL_HORIZON_HOURS\":\"24\",\"--DROP_INCOMPLETE_ROWS\":\"true\"}" \
    --max-retries 0 \
    --timeout 60 \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    "${AWS_ARGS[@]}" >/dev/null
fi

if aws glue get-crawler --name "${CRAWLER_NAME}" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
  echo "Actualizando crawler ${CRAWLER_NAME}"
  aws glue update-crawler \
    --name "${CRAWLER_NAME}" \
    --role "${ROLE_ARN}" \
    --database-name "${DATABASE_NAME}" \
    --targets "{\"S3Targets\":[{\"Path\":\"${TARGET_PATH}\"}]}" \
    --configuration "${CRAWLER_CONFIGURATION}" \
    --schema-change-policy '{"UpdateBehavior":"UPDATE_IN_DATABASE","DeleteBehavior":"DEPRECATE_IN_DATABASE"}' \
    "${AWS_ARGS[@]}" >/dev/null
else
  echo "Creando crawler ${CRAWLER_NAME}"
  aws glue create-crawler \
    --name "${CRAWLER_NAME}" \
    --role "${ROLE_ARN}" \
    --database-name "${DATABASE_NAME}" \
    --targets "{\"S3Targets\":[{\"Path\":\"${TARGET_PATH}\"}]}" \
    --configuration "${CRAWLER_CONFIGURATION}" \
    --schema-change-policy '{"UpdateBehavior":"UPDATE_IN_DATABASE","DeleteBehavior":"DEPRECATE_IN_DATABASE"}' \
    "${AWS_ARGS[@]}" >/dev/null
fi

echo "Listo."
echo "Role ARN: ${ROLE_ARN}"
echo "Glue database: ${DATABASE_NAME}"
echo "Glue job: ${JOB_NAME}"
echo "Glue crawler: ${CRAWLER_NAME}"

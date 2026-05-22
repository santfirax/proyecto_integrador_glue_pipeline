#!/usr/bin/env bash

set -euo pipefail

PROFILE="${AWS_PROFILE_NAME:-}"
REGION="${AWS_REGION_NAME:-us-east-1}"
JOB_NAME="${GLUE_JOB_NAME:-simem-bronze-to-silver}"
CRAWLER_NAME="${GLUE_CRAWLER_NAME:-simem-silver-crawler}"
DATASET_SLUGS="${DATASET_SLUGS:-}"
WAIT_FOR_JOB="${WAIT_FOR_JOB:-true}"
START_CRAWLER="${START_CRAWLER:-false}"
WAIT_FOR_CRAWLER="${WAIT_FOR_CRAWLER:-true}"
EXTRA_GLUE_ARGS="${EXTRA_GLUE_ARGS:-}"
SILVER_TARGET_BUCKET="${SILVER_TARGET_BUCKET:-}"
SILVER_TARGET_PREFIX="${SILVER_TARGET_PREFIX:-}"

AWS_ARGS=(--region "${REGION}")

if [[ -n "${PROFILE}" ]]; then
  AWS_ARGS+=(--profile "${PROFILE}")
fi

JOB_ARGS='{}'

if [[ -n "${DATASET_SLUGS}" ]]; then
  JOB_ARGS="{\"--DATASET_SLUGS\":\"${DATASET_SLUGS}\"}"
fi

if [[ -n "${EXTRA_GLUE_ARGS}" ]]; then
  if [[ "${JOB_ARGS}" == "{}" ]]; then
    JOB_ARGS="${EXTRA_GLUE_ARGS}"
  else
    JOB_ARGS="$(python3 - <<'PY' "${JOB_ARGS}" "${EXTRA_GLUE_ARGS}"
import json
import sys

current_args = json.loads(sys.argv[1])
extra_args = json.loads(sys.argv[2])
current_args.update(extra_args)
print(json.dumps(current_args, separators=(",", ":")))
PY
)"
  fi
fi

echo "Iniciando Glue job ${JOB_NAME}"
RUN_ID="$(aws glue start-job-run \
  --job-name "${JOB_NAME}" \
  --arguments "${JOB_ARGS}" \
  "${AWS_ARGS[@]}" \
  --query 'JobRunId' \
  --output text)"

echo "JobRunId: ${RUN_ID}"

if [[ "${WAIT_FOR_JOB}" == "true" ]]; then
  while true; do
    STATE="$(aws glue get-job-run \
      --job-name "${JOB_NAME}" \
      --run-id "${RUN_ID}" \
      "${AWS_ARGS[@]}" \
      --query 'JobRun.JobRunState' \
      --output text)"

    echo "Estado job: ${STATE}"

    case "${STATE}" in
      SUCCEEDED)
        break
        ;;
      FAILED|STOPPED|TIMEOUT|ERROR|EXPIRED)
        echo "El job termino en estado ${STATE}" >&2
        exit 1
        ;;
    esac

    sleep 30
  done
fi

if [[ "${START_CRAWLER}" == "true" ]]; then
  if [[ "${CRAWLER_NAME}" == "simem-silver-crawler" ]]; then
    if [[ -z "${SILVER_TARGET_BUCKET}" ]]; then
      SILVER_TARGET_BUCKET="${S3_BUCKET_NAME:-eafit-proyecto-integrador-simem}"
    fi

    if [[ -z "${SILVER_TARGET_PREFIX}" ]]; then
      SILVER_TARGET_PREFIX="silver/simem-data/"
    fi

    echo "Actualizando targets del crawler ${CRAWLER_NAME} desde s3://${SILVER_TARGET_BUCKET}/${SILVER_TARGET_PREFIX}"
    python3 scripts/refresh_silver_crawler_targets.py \
      --crawler-name "${CRAWLER_NAME}" \
      --bucket "${SILVER_TARGET_BUCKET}" \
      --prefix "${SILVER_TARGET_PREFIX}" \
      --region "${REGION}"
  fi

  echo "Iniciando crawler ${CRAWLER_NAME}"
  aws glue start-crawler \
    --name "${CRAWLER_NAME}" \
    "${AWS_ARGS[@]}"

  if [[ "${WAIT_FOR_CRAWLER}" == "true" ]]; then
    while true; do
      STATE="$(aws glue get-crawler \
        --name "${CRAWLER_NAME}" \
        "${AWS_ARGS[@]}" \
        --query 'Crawler.State' \
        --output text)"

      echo "Estado crawler: ${STATE}"

      if [[ "${STATE}" == "READY" ]]; then
        break
      fi

      sleep 20
    done
  fi
fi

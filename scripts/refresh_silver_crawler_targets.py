#!/usr/bin/env python3
"""
Actualiza los S3Targets del crawler de silver para que apunten a una carpeta por dataset.

Esto evita que Glue agrupe distintos datasets compatibles bajo una sola tabla como `simem_data`.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List

import boto3
from botocore.exceptions import ClientError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Actualiza el crawler de silver con un target por dataset encontrado en S3."
    )
    parser.add_argument("--crawler-name", required=True, help="Nombre del crawler de Glue.")
    parser.add_argument("--bucket", required=True, help="Bucket S3 donde vive silver.")
    parser.add_argument(
        "--prefix",
        required=True,
        help="Prefijo raiz de silver, por ejemplo silver/simem-data/",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Region AWS. Si se omite usa la sesion por defecto.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo imprime los targets descubiertos.",
    )
    return parser.parse_args()


def normalize_prefix(prefix: str) -> str:
    return prefix.strip("/").rstrip("/") + "/"


def list_dataset_prefixes(s3_client, bucket: str, prefix: str) -> List[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    common_prefixes: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for item in page.get("CommonPrefixes", []):
            current_prefix = item["Prefix"]
            relative = current_prefix[len(prefix) :].strip("/")

            if not relative:
                continue

            common_prefixes.append(current_prefix)

    return sorted(set(common_prefixes))


def main() -> int:
    args = parse_args()
    session = boto3.session.Session(region_name=args.region)
    s3_client = session.client("s3")
    glue_client = session.client("glue")

    prefix = normalize_prefix(args.prefix)
    dataset_prefixes = list_dataset_prefixes(s3_client, args.bucket, prefix)

    if not dataset_prefixes:
        print(
            f"No se encontraron dataset prefixes bajo s3://{args.bucket}/{prefix}",
            file=sys.stderr,
        )
        return 1

    targets = {
        "S3Targets": [
            {"Path": f"s3://{args.bucket}/{dataset_prefix}"}
            for dataset_prefix in dataset_prefixes
        ]
    }

    print(json.dumps(targets, indent=2, ensure_ascii=True))

    if args.dry_run:
        return 0

    try:
        glue_client.update_crawler(
            Name=args.crawler_name,
            Targets=targets,
            Configuration=json.dumps({"Version": 1.0}),
        )
    except ClientError as error:
        print(str(error), file=sys.stderr)
        return 1

    print(f"Crawler {args.crawler_name} actualizado con {len(dataset_prefixes)} targets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

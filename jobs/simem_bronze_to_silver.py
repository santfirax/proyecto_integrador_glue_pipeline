import re
import sys
import unicodedata
from collections import defaultdict
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


FILE_REGEX = re.compile(
    r"(?P<year>\d{4})/(?P<month>\d{2})/"
    r"(?P<dataset_slug>[a-z0-9-]+)-"
    r"(?P<period_start>\d{4}-\d{2}-\d{2})_"
    r"(?P<period_end>\d{4}-\d{2}-\d{2})\.json$"
)


def get_optional_arg(name, default=None):
    flag = f"--{name}"

    if flag not in sys.argv:
        return default

    position = sys.argv.index(flag)

    if position + 1 >= len(sys.argv):
        return default

    return sys.argv[position + 1]


def parse_s3_uri(uri):
    parsed = urlparse(uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Ruta S3 invalida: {uri}")

    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/")
    return bucket, prefix


def list_matching_objects(bucket, prefix, dataset_filters):
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    grouped_keys = defaultdict(list)

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            relative_key = key[len(prefix) + 1 :] if prefix else key
            match = FILE_REGEX.match(relative_key)

            if not match:
                continue

            dataset_slug = match.group("dataset_slug")

            if dataset_filters and dataset_slug not in dataset_filters:
                continue

            grouped_keys[dataset_slug].append(f"s3://{bucket}/{key}")

    return grouped_keys


def normalize_column_name(name):
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", normalized).strip("_").lower()

    if not normalized:
        normalized = "column"

    if normalized[0].isdigit():
        normalized = f"col_{normalized}"

    return normalized


def sanitize_dataframe_columns(dataframe):
    result = dataframe
    occurrences = defaultdict(int)

    for original_name in dataframe.columns:
        base_name = normalize_column_name(original_name)
        occurrences[base_name] += 1
        sanitized_name = base_name if occurrences[base_name] == 1 else f"{base_name}_{occurrences[base_name]}"

        if original_name != sanitized_name:
            result = result.withColumnRenamed(original_name, sanitized_name)

    return result


def add_metadata_columns(dataframe, dataset_slug):
    source_file_column = F.input_file_name()

    return (
        dataframe.withColumn("source_file", source_file_column)
        .withColumn("dataset_slug", F.lit(dataset_slug))
        .withColumn("year", F.regexp_extract(source_file_column, r"/(\d{4})/\d{2}/", 1).cast("int"))
        .withColumn("month", F.regexp_extract(source_file_column, r"/\d{4}/(\d{2})/", 1).cast("int"))
        .withColumn(
            "period_start",
            F.to_date(F.regexp_extract(source_file_column, r"-(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}\.json$", 1)),
        )
        .withColumn(
            "period_end",
            F.to_date(F.regexp_extract(source_file_column, r"-\d{4}-\d{2}-\d{2}_(\d{4}-\d{2}-\d{2})\.json$", 1)),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def process_dataset(spark, dataset_slug, source_paths, target_path):
    dataframe = spark.read.option("multiLine", True).json(source_paths)
    dataframe = sanitize_dataframe_columns(dataframe)
    dataframe = add_metadata_columns(dataframe, dataset_slug)

    dataset_target_path = f"{target_path.rstrip('/')}/{dataset_slug}/"

    (
        dataframe.write.mode("overwrite")
        .format("parquet")
        .partitionBy("year", "month")
        .save(dataset_target_path)
    )

    return dataframe.count()


required_args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])
dataset_slugs_arg = get_optional_arg("DATASET_SLUGS", "")

dataset_filters = {item.strip() for item in dataset_slugs_arg.split(",") if item.strip()}

source_bucket, source_prefix = parse_s3_uri(required_args["SOURCE_PATH"])
source_groups = list_matching_objects(source_bucket, source_prefix, dataset_filters)

if not source_groups:
    raise RuntimeError(
        f"No se encontraron archivos JSON en {required_args['SOURCE_PATH']} "
        f"para los datasets: {dataset_slugs_arg or 'todos'}"
    )

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(required_args["JOB_NAME"], required_args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

total_rows = 0

for dataset_slug, source_paths in sorted(source_groups.items()):
    print(f"Procesando dataset {dataset_slug} con {len(source_paths)} archivos")
    row_count = process_dataset(
        spark=spark,
        dataset_slug=dataset_slug,
        source_paths=source_paths,
        target_path=required_args["TARGET_PATH"],
    )
    total_rows += row_count
    print(f"Dataset {dataset_slug} escrito correctamente. Registros: {row_count}")

print(f"Proceso Glue finalizado. Registros totales escritos: {total_rows}")
job.commit()

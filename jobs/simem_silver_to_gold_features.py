import re
import sys
import unicodedata
from functools import reduce

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window


DEFAULT_TARGET_DATASET_SLUG = "demanda-real"
DEFAULT_LABEL_HORIZON_HOURS = 24
DEFAULT_VERSION_PRIORITY = ["TXR", "TXF", "TX7", "TX6", "TX5", "TX4", "TX3", "TX2", "TX1"]


def get_optional_arg(name, default=None):
    flag = f"--{name}"

    if flag not in sys.argv:
        return default

    position = sys.argv.index(flag)

    if position + 1 >= len(sys.argv):
        return default

    return sys.argv[position + 1]


def normalize_name(value):
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", normalized).strip("_").lower()

    if not normalized:
        normalized = "column"

    if normalized[0].isdigit():
        normalized = f"col_{normalized}"

    return normalized


def sanitize_dataframe_columns(dataframe):
    renamed = dataframe
    occurrences = {}

    for original_name in dataframe.columns:
        base_name = normalize_name(original_name)
        occurrences[base_name] = occurrences.get(base_name, 0) + 1
        new_name = base_name if occurrences[base_name] == 1 else f"{base_name}_{occurrences[base_name]}"

        if original_name != new_name:
            renamed = renamed.withColumnRenamed(original_name, new_name)

    return renamed


def prefix_non_key_columns(dataframe, prefix, key_columns):
    renamed = dataframe

    for column_name in dataframe.columns:
        if column_name in key_columns:
            continue

        renamed = renamed.withColumnRenamed(column_name, f"{prefix}_{normalize_name(column_name)}")

    return renamed


def read_dataset(spark, source_path, dataset_slug):
    return spark.read.parquet(f"{source_path.rstrip('/')}/{dataset_slug}/")


def build_version_priority_expression(version_column):
    mapping = []
    total_priorities = len(DEFAULT_VERSION_PRIORITY)

    for index, version_name in enumerate(DEFAULT_VERSION_PRIORITY):
        mapping.extend([F.lit(version_name), F.lit(total_priorities - index)])

    return F.coalesce(
        F.create_map(*mapping)[F.upper(F.col(version_column).cast("string"))],
        F.lit(0),
    )


def deduplicate_versioned_rows(dataframe, key_columns):
    available_key_columns = [column_name for column_name in key_columns if column_name in dataframe.columns]

    if "version" not in dataframe.columns or not available_key_columns:
        return dataframe

    ordering = [build_version_priority_expression("version").desc()]

    if "processed_at" in dataframe.columns:
        ordering.append(F.col("processed_at").cast("timestamp").desc_nulls_last())

    if "source_file" in dataframe.columns:
        ordering.append(F.col("source_file").desc_nulls_last())

    ordering.append(F.upper(F.col("version").cast("string")).desc_nulls_last())

    ranked_window = Window.partitionBy(*available_key_columns).orderBy(*ordering)

    return (
        dataframe.withColumn("_version_rank", F.row_number().over(ranked_window))
        .filter(F.col("_version_rank") == 1)
        .drop("_version_rank")
    )


def aggregate_target_hourly(dataframe, metric_prefix):
    base = dataframe.withColumn("event_ts", F.to_timestamp("fechahora"))

    total = (
        base.groupBy("event_ts")
        .agg(
            F.sum("valor").alias(metric_prefix),
            F.countDistinct("codigosicagente").alias(f"{metric_prefix}_agentes"),
        )
    )

    by_market = base.groupBy("event_ts").pivot("tipomercado").agg(F.sum("valor"))
    by_market = prefix_non_key_columns(by_market, metric_prefix, {"event_ts"})

    return sanitize_dataframe_columns(total.join(by_market, "event_ts", "left"))


def aggregate_feature_hourly(dataframe, metric_prefix):
    base = dataframe.withColumn("event_ts", F.to_timestamp("fechahora"))
    by_market = base.groupBy("event_ts").pivot("tipomercado").agg(F.sum("valor"))
    by_market = prefix_non_key_columns(by_market, metric_prefix, {"event_ts"})

    total = (
        base.groupBy("event_ts")
        .agg(
            F.sum("valor").alias(f"{metric_prefix}_total"),
            F.countDistinct("codigosicagente").alias(f"{metric_prefix}_agentes"),
        )
    )

    return sanitize_dataframe_columns(total.join(by_market, "event_ts", "left"))


def aggregate_generation_hourly(dataframe):
    base = dataframe.withColumn("event_ts", F.to_timestamp("fechahora"))

    return base.groupBy("event_ts").agg(
        F.sum("valor").alias("generacion_real_total"),
        F.countDistinct("codigoplanta").alias("generacion_real_plantas"),
        F.countDistinct("codigosicagente").alias("generacion_real_agentes"),
    )


def aggregate_hydrology_daily(dataframe):
    base = dataframe.withColumn("event_date", F.to_date("fecha"))

    return base.groupBy("event_date").agg(
        F.sum("aporteshidricosenergia").alias("aporte_hidrico_energia_total"),
        F.sum("mediahistoricaenergia").alias("aporte_hidrico_media_historica_total"),
        F.sum("promedioacumuladoenergia").alias("aporte_hidrico_promedio_acumulado_total"),
        F.countDistinct("regionhidrologica").alias("aporte_hidrico_regiones"),
    )


def aggregate_units_daily(dataframe):
    base = dataframe.withColumn("event_date", F.to_date("fecha"))

    totals = base.groupBy("event_date").agg(
        F.countDistinct("codigounidadgeneracion").alias("unidades_total"),
        F.countDistinct("codigoplanta").alias("plantas_total"),
        F.countDistinct(
            F.when(F.lower("estadorecurso") == F.lit("operacion"), F.col("codigounidadgeneracion"))
        ).alias("unidades_operacion"),
    )

    by_type = (
        base.select("event_date", "codigounidadgeneracion", "tipogeneracion")
        .dropDuplicates()
        .groupBy("event_date")
        .pivot("tipogeneracion")
        .agg(F.count("*"))
    )

    by_type = prefix_non_key_columns(by_type, "unidades_tipo", {"event_date"})

    return sanitize_dataframe_columns(totals.join(by_type, "event_date", "left"))


def build_hourly_spine(spark, target_dataframe):
    bounds = target_dataframe.agg(
        F.min("event_ts").alias("min_ts"),
        F.max("event_ts").alias("max_ts"),
    ).first()

    return spark.range(1).select(
        F.explode(
            F.sequence(F.lit(bounds["min_ts"]), F.lit(bounds["max_ts"]), F.expr("INTERVAL 1 HOUR"))
        ).alias("event_ts")
    )


def add_calendar_features(dataframe):
    enriched = (
        dataframe.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("day_of_week", F.dayofweek("event_ts"))
        .withColumn("day_of_month", F.dayofmonth("event_ts"))
        .withColumn("day_of_year", F.dayofyear("event_ts"))
        .withColumn("week_of_year", F.weekofyear("event_ts"))
        .withColumn("month_of_year", F.month("event_ts"))
        .withColumn("is_weekend", F.when(F.dayofweek("event_ts").isin(1, 7), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_month_start", F.when(F.dayofmonth("event_ts") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "is_month_end",
            F.when(F.to_date("event_ts") == F.last_day(F.to_date("event_ts")), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    return enriched.withColumn(
        "aporte_hidrico_vs_media_ratio",
        F.when(
            F.col("aporte_hidrico_media_historica_total").isNotNull()
            & (F.col("aporte_hidrico_media_historica_total") != 0),
            F.col("aporte_hidrico_energia_total") / F.col("aporte_hidrico_media_historica_total"),
        ),
    ).withColumn(
        "unidades_operacion_share",
        F.when(F.col("unidades_total").isNotNull() & (F.col("unidades_total") != 0), F.col("unidades_operacion") / F.col("unidades_total")),
    )


def add_lag_features(dataframe, target_column, label_horizon_hours):
    ordered_window = Window.orderBy("event_ts")
    last_24_window = ordered_window.rowsBetween(-24, -1)
    last_168_window = ordered_window.rowsBetween(-168, -1)

    label_column = f"label_{normalize_name(target_column)}_h_plus_{label_horizon_hours}"

    result = (
        dataframe.withColumn(label_column, F.lead(target_column, label_horizon_hours).over(ordered_window))
        .withColumn(f"{target_column}_lag_1h", F.lag(target_column, 1).over(ordered_window))
        .withColumn(f"{target_column}_lag_24h", F.lag(target_column, 24).over(ordered_window))
        .withColumn(f"{target_column}_lag_168h", F.lag(target_column, 168).over(ordered_window))
        .withColumn(f"{target_column}_rolling_mean_24h", F.avg(target_column).over(last_24_window))
        .withColumn(f"{target_column}_rolling_stddev_24h", F.stddev(target_column).over(last_24_window))
        .withColumn(f"{target_column}_rolling_mean_168h", F.avg(target_column).over(last_168_window))
        .withColumn("demanda_comercial_total_lag_24h", F.lag("demanda_comercial_total", 24).over(ordered_window))
        .withColumn("generacion_real_total_lag_24h", F.lag("generacion_real_total", 24).over(ordered_window))
    )

    return result, label_column


def build_gold_dataset(spark, source_path, target_dataset_slug, label_horizon_hours, drop_incomplete_rows):
    target_metric_prefix = normalize_name(target_dataset_slug)
    target_column = f"target_{target_metric_prefix}"

    target_source = deduplicate_versioned_rows(
        read_dataset(spark, source_path, target_dataset_slug),
        ["fechahora", "codigosicagente", "tipomercado", "codigovariable", "codigoduracion", "unidadmedida"],
    )
    commercial_source = deduplicate_versioned_rows(
        read_dataset(spark, source_path, "demanda-comercial"),
        ["fechahora", "codigosicagente", "tipomercado", "codigovariable", "codigoduracion", "unidadmedida"],
    )
    generation_source = deduplicate_versioned_rows(
        read_dataset(spark, source_path, "generacion-real"),
        ["fechahora", "codigoplanta", "codigosicagente", "codigovariable", "codigoduracion", "unidadmedida"],
    )

    target_hourly = aggregate_target_hourly(target_source, target_column)
    commercial_hourly = aggregate_feature_hourly(commercial_source, "demanda_comercial")
    generation_hourly = aggregate_generation_hourly(generation_source)
    hydrology_daily = aggregate_hydrology_daily(read_dataset(spark, source_path, "aporte-hidricos"))
    units_daily = aggregate_units_daily(read_dataset(spark, source_path, "unidades-generacion"))

    hourly_spine = build_hourly_spine(spark, target_hourly)

    hourly = reduce(
        lambda left, right: left.join(right, "event_ts", "left"),
        [hourly_spine, target_hourly, commercial_hourly, generation_hourly],
    )

    enriched = (
        hourly.withColumn("event_date", F.to_date("event_ts"))
        .join(hydrology_daily, "event_date", "left")
        .join(units_daily, "event_date", "left")
        .drop("event_date")
    )

    enriched = add_calendar_features(enriched)
    enriched, label_column = add_lag_features(enriched, target_column, label_horizon_hours)
    enriched = sanitize_dataframe_columns(enriched)

    if drop_incomplete_rows:
        enriched = enriched.dropna(
            subset=[
                target_column,
                label_column,
                f"{target_column}_lag_24h",
                f"{target_column}_lag_168h",
                "demanda_comercial_total",
                "generacion_real_total",
                "aporte_hidrico_energia_total",
                "unidades_total",
            ]
        )

    return (
        enriched.withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("processed_at", F.current_timestamp())
    )


required_args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])
target_dataset_slug = get_optional_arg("TARGET_DATASET_SLUG", DEFAULT_TARGET_DATASET_SLUG)
label_horizon_hours = int(get_optional_arg("LABEL_HORIZON_HOURS", str(DEFAULT_LABEL_HORIZON_HOURS)))
drop_incomplete_rows = str(get_optional_arg("DROP_INCOMPLETE_ROWS", "true")).lower() == "true"

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(required_args["JOB_NAME"], required_args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

gold_dataset = build_gold_dataset(
    spark=spark,
    source_path=required_args["SOURCE_PATH"],
    target_dataset_slug=target_dataset_slug,
    label_horizon_hours=label_horizon_hours,
    drop_incomplete_rows=drop_incomplete_rows,
)

(
    gold_dataset.write.mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month")
    .save(required_args["TARGET_PATH"])
)

print(
    f"Gold dataset escrito correctamente en {required_args['TARGET_PATH']} "
    f"con target {target_dataset_slug} y horizonte {label_horizon_hours}h"
)
job.commit()

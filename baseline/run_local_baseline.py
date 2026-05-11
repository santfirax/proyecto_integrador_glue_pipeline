import argparse
import json
import math
from pathlib import Path

import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def parse_args():
    parser = argparse.ArgumentParser(
        description="Corre un baseline local sobre la capa gold en parquet."
    )
    parser.add_argument(
        "--input-path",
        required=True,
        help="Ruta local a un archivo parquet o a una carpeta con parquet.",
    )
    parser.add_argument(
        "--timestamp-column",
        default="event_ts",
        help="Columna temporal usada para ordenar y hacer split.",
    )
    parser.add_argument(
        "--target-column",
        default="",
        help="Columna label objetivo. Si se omite, se infiere la primera que empiece por label_.",
    )
    parser.add_argument(
        "--validation-size",
        type=float,
        default=0.15,
        help="Proporcion del dataset para validacion temporal.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.15,
        help="Proporcion del dataset para test temporal.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=0,
        help="Si es mayor a 0, limita el dataset a las ultimas N filas ordenadas por fecha.",
    )
    parser.add_argument(
        "--output-dir",
        default="baseline/reports",
        help="Carpeta donde se escribe el reporte JSON.",
    )
    return parser.parse_args()


def load_parquet_dataset(input_path):
    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"No existe la ruta: {path}")

    if path.is_file():
        parquet_files = [path]
    else:
        parquet_files = sorted(path.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No se encontraron archivos parquet en {path}")

    frames = [pd.read_parquet(file_path) for file_path in parquet_files]
    return pd.concat(frames, ignore_index=True)


def infer_target_column(dataframe):
    label_columns = sorted(column for column in dataframe.columns if column.startswith("label_"))

    if not label_columns:
        raise ValueError("No se encontro ninguna columna label_ en el dataset.")

    return label_columns[0]


def validate_split_sizes(validation_size, test_size):
    if validation_size <= 0 or test_size <= 0:
        raise ValueError("validation-size y test-size deben ser mayores que 0.")

    if validation_size + test_size >= 1:
        raise ValueError("validation-size + test-size debe ser menor que 1.")


def summarize_nulls(dataframe):
    summary = pd.DataFrame(
        {
            "column": dataframe.columns,
            "null_count": dataframe.isna().sum().values,
            "null_pct": (dataframe.isna().mean() * 100).round(2).values,
        }
    )

    return summary.sort_values(["null_pct", "null_count"], ascending=False).reset_index(drop=True)


def drop_non_target_label_columns(dataframe, target_column):
    ignored_label_columns = [
        column for column in dataframe.columns if column.startswith("label_") and column != target_column
    ]

    cleaned = dataframe.drop(columns=ignored_label_columns, errors="ignore")
    return cleaned, ignored_label_columns


def summarize_granularity(dataframe, timestamp_column):
    timestamps = pd.to_datetime(dataframe[timestamp_column], errors="coerce").dropna().sort_values()

    if timestamps.empty:
        raise ValueError(f"La columna temporal {timestamp_column} no tiene timestamps validos.")

    diffs = timestamps.diff().dropna()
    duplicates = int(timestamps.duplicated().sum())

    if diffs.empty:
        expected_delta = pd.Timedelta(hours=1)
    else:
        expected_delta = diffs.mode().iloc[0]

    if pd.isna(expected_delta) or expected_delta <= pd.Timedelta(0):
        expected_delta = pd.Timedelta(hours=1)

    full_range = pd.date_range(timestamps.iloc[0], timestamps.iloc[-1], freq=expected_delta)
    missing_count = max(len(full_range) - timestamps.nunique(), 0)

    return {
        "min_timestamp": timestamps.iloc[0].isoformat(),
        "max_timestamp": timestamps.iloc[-1].isoformat(),
        "row_count": int(len(dataframe)),
        "distinct_timestamps": int(timestamps.nunique()),
        "duplicate_timestamps": duplicates,
        "expected_frequency_minutes": round(expected_delta.total_seconds() / 60, 2),
        "missing_timestamp_count": int(missing_count),
    }


def summarize_feature_means_by_split(train_df, validation_df, test_df, feature_columns):
    split_frames = {
        "train": train_df,
        "validation": validation_df,
        "test": test_df,
    }

    summary = {}

    for split_name, split_df in split_frames.items():
        summary[split_name] = {
            column: round(float(split_df[column].mean()), 6)
            for column in feature_columns
            if column in split_df.columns
        }

    return summary


def summarize_monthly_drift(dataframe, timestamp_column, focus_columns):
    monthly = dataframe.copy()
    monthly["event_month"] = monthly[timestamp_column].dt.to_period("M").astype(str)

    aggregated = monthly.groupby("event_month")[focus_columns].mean().reset_index()
    drift_frames = []

    for column in focus_columns:
        current = aggregated[["event_month", column]].copy()
        current["previous_mean"] = current[column].shift(1)
        current["change_pct"] = (
            ((current[column] - current["previous_mean"]) / current["previous_mean"])
            .replace([float("inf"), float("-inf")], pd.NA)
            * 100
        )
        current["metric"] = column
        drift_frames.append(current.rename(columns={column: "current_mean"}))

    drift = pd.concat(drift_frames, ignore_index=True)
    drift["abs_change_pct"] = drift["change_pct"].abs()
    drift = drift.dropna(subset=["change_pct"]).sort_values("abs_change_pct", ascending=False)

    return {
        "monthly_means_tail": aggregated.tail(12).round(6).to_dict(orient="records"),
        "largest_month_over_month_changes": drift.head(10).round(6).to_dict(orient="records"),
    }


def temporal_split(dataframe, validation_size, test_size):
    total_rows = len(dataframe)
    train_end = math.floor(total_rows * (1 - validation_size - test_size))
    validation_end = math.floor(total_rows * (1 - test_size))

    train_df = dataframe.iloc[:train_end].copy()
    validation_df = dataframe.iloc[train_end:validation_end].copy()
    test_df = dataframe.iloc[validation_end:].copy()

    return train_df, validation_df, test_df


def build_feature_columns(dataframe, target_column, timestamp_column):
    numeric_columns = dataframe.select_dtypes(include=["number"]).columns.tolist()
    excluded_columns = {target_column}
    excluded_columns.update(column for column in dataframe.columns if column.startswith("label_") and column != target_column)

    feature_columns = [column for column in numeric_columns if column not in excluded_columns]

    if not feature_columns:
        raise ValueError("No quedaron columnas numericas para entrenar el baseline.")

    return feature_columns


def infer_naive_prediction_column(target_column, feature_columns):
    if not target_column.startswith("label_"):
        return ""

    base_target = target_column[len("label_") :].split("_h_plus_")[0]

    if base_target in feature_columns:
        return base_target

    candidate = f"{base_target}_lag_24h"

    if candidate in feature_columns:
        return candidate

    return ""


def compute_metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = math.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    denominator = y_true.abs().replace(0, pd.NA)
    mape = ((y_true - y_pred).abs() / denominator).dropna().mean() * 100

    return {
        "mae": round(float(mae), 6),
        "rmse": round(float(rmse), 6),
        "r2": round(float(r2), 6),
        "mape_pct": round(float(mape), 6) if pd.notna(mape) else None,
    }


def run_baseline_models(train_df, validation_df, test_df, feature_columns, target_column):
    x_train = train_df[feature_columns]
    y_train = train_df[target_column]
    x_validation = validation_df[feature_columns]
    y_validation = validation_df[target_column]
    x_test = test_df[feature_columns]
    y_test = test_df[target_column]

    models = {
        "dummy_mean": DummyRegressor(strategy="mean"),
        "ridge_regression_scaled": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", Ridge(alpha=1.0)),
            ]
        ),
        "random_forest": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestRegressor(
                        n_estimators=300,
                        max_depth=14,
                        min_samples_leaf=2,
                        n_jobs=-1,
                        random_state=42,
                    ),
                ),
            ]
        ),
    }

    results = {}

    for model_name, model in models.items():
        model.fit(x_train, y_train)
        validation_pred = pd.Series(model.predict(x_validation), index=y_validation.index)
        test_pred = pd.Series(model.predict(x_test), index=y_test.index)

        results[model_name] = {
            "validation": compute_metrics(y_validation, validation_pred),
            "test": compute_metrics(y_test, test_pred),
        }

    naive_column = infer_naive_prediction_column(target_column, feature_columns)

    if naive_column:
        validation_naive = y_validation.index.to_series().map(validation_df[naive_column]).astype(float)
        test_naive = y_test.index.to_series().map(test_df[naive_column]).astype(float)

        valid_validation = validation_naive.notna() & y_validation.notna()
        valid_test = test_naive.notna() & y_test.notna()

        results["naive_persistence"] = {
            "prediction_column": naive_column,
            "validation": compute_metrics(y_validation[valid_validation], validation_naive[valid_validation]),
            "test": compute_metrics(y_test[valid_test], test_naive[valid_test]),
        }

    return results


def summarize_split(name, dataframe, timestamp_column):
    return {
        "rows": int(len(dataframe)),
        "start": dataframe[timestamp_column].iloc[0].isoformat() if len(dataframe) else None,
        "end": dataframe[timestamp_column].iloc[-1].isoformat() if len(dataframe) else None,
    }


def main():
    args = parse_args()
    validate_split_sizes(args.validation_size, args.test_size)

    dataframe = load_parquet_dataset(args.input_path)
    target_column = args.target_column or infer_target_column(dataframe)
    dataframe, ignored_label_columns = drop_non_target_label_columns(dataframe, target_column)

    dataframe[args.timestamp_column] = pd.to_datetime(dataframe[args.timestamp_column], errors="coerce")
    dataframe = dataframe.dropna(subset=[args.timestamp_column, target_column]).sort_values(args.timestamp_column).reset_index(drop=True)

    if args.sample_rows and args.sample_rows > 0:
        dataframe = dataframe.tail(args.sample_rows).reset_index(drop=True)

    null_summary = summarize_nulls(dataframe)
    granularity_summary = summarize_granularity(dataframe, args.timestamp_column)

    train_df, validation_df, test_df = temporal_split(dataframe, args.validation_size, args.test_size)
    feature_columns = build_feature_columns(dataframe, target_column, args.timestamp_column)
    model_results = run_baseline_models(train_df, validation_df, test_df, feature_columns, target_column)
    split_mean_columns = [
        column
        for column in [
            "target_demanda_real",
            target_column,
            "demanda_comercial_total",
            "generacion_real_total",
            "aporte_hidrico_energia_total",
            "unidades_operacion",
        ]
        if column in dataframe.columns
    ]
    monthly_drift = summarize_monthly_drift(dataframe, args.timestamp_column, split_mean_columns)
    split_feature_means = summarize_feature_means_by_split(train_df, validation_df, test_df, split_mean_columns)

    report = {
        "input_path": str(args.input_path),
        "target_column": target_column,
        "timestamp_column": args.timestamp_column,
        "dataset_shape": {
            "rows": int(len(dataframe)),
            "columns": int(len(dataframe.columns)),
        },
        "granularity": granularity_summary,
        "splits": {
            "train": summarize_split("train", train_df, args.timestamp_column),
            "validation": summarize_split("validation", validation_df, args.timestamp_column),
            "test": summarize_split("test", test_df, args.timestamp_column),
        },
        "feature_columns": feature_columns,
        "ignored_label_columns": ignored_label_columns,
        "top_null_columns": null_summary.head(20).to_dict(orient="records"),
        "split_feature_means": split_feature_means,
        "monthly_drift": monthly_drift,
        "models": model_results,
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "baseline_report.json"
    nulls_path = output_dir / "null_summary.csv"

    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True))
    null_summary.to_csv(nulls_path, index=False)

    print(json.dumps(report, indent=2, ensure_ascii=True))
    print(f"\nReporte escrito en: {report_path}")
    print(f"Resumen de nulos escrito en: {nulls_path}")


if __name__ == "__main__":
    main()

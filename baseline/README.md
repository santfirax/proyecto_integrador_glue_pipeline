# Local Baseline

Esta carpeta sirve para validar localmente que la capa `gold` ya esta lista para modelado antes de meter SageMaker.

## Que hace

El script [run_local_baseline.py](/Users/santiagomolano/thinklp/simem-glue-pipeline/baseline/run_local_baseline.py):

- carga parquet local de la capa `gold`
- revisa nulos
- revisa granularidad temporal
- hace split temporal `train / validation / test`
- entrena un baseline muy simple:
  - `DummyRegressor`
  - `Ridge` con escalado
  - `RandomForestRegressor`
  - baseline naive si existe una columna razonable de persistencia
- escribe un reporte JSON y un CSV con el resumen de nulos

## Instalar dependencias

Con el entorno que ya montamos:

```bash
/Users/santiagomolano/thinklp/simem-glue-pipeline/.venv/bin/pip install -r \
  /Users/santiagomolano/thinklp/simem-glue-pipeline/baseline/requirements.txt
```

## Traer datos gold a local

Cuando tengas credenciales activas de AWS, puedes bajar la capa `gold` asi:

```bash
mkdir -p /Users/santiagomolano/thinklp/simem-glue-pipeline/data/gold
aws s3 sync \
  s3://eafit-proyecto-integrador-simem/gold/simem-features/demanda-real-hourly/ \
  /Users/santiagomolano/thinklp/simem-glue-pipeline/data/gold/demanda-real-hourly/ \
  --profile 878311411214_AdministratorAccess
```

## Ejecutar baseline

```bash
/Users/santiagomolano/thinklp/simem-glue-pipeline/.venv/bin/python \
  /Users/santiagomolano/thinklp/simem-glue-pipeline/baseline/run_local_baseline.py \
  --input-path /Users/santiagomolano/thinklp/simem-glue-pipeline/data/gold/demanda-real-hourly/ \
  --target-column label_target_demanda_real_h_plus_24 \
  --output-dir /Users/santiagomolano/thinklp/simem-glue-pipeline/baseline/reports
```

## Que mirar en el reporte

- `top_null_columns`
  Mira si el target o features clave llegan demasiado vacios.
- `granularity`
  Te dice si la serie es realmente horaria y si hay timestamps faltantes o duplicados.
- `splits`
  Confirma que el corte fue temporal y no aleatorio.
- `split_feature_means`
  Muestra si el nivel promedio del target o de features clave cambia fuerte entre `train`, `validation` y `test`.
- `monthly_drift`
  Resume promedios mensuales y los mayores saltos mes a mes para detectar cambios de regimen o duplicidades.
- `models`
  Si ni `ridge_regression_scaled` ni `random_forest` superan al baseline naive, el problema o las features todavia no estan bien armados.

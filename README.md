# SIMEM Glue Pipeline

Proyecto separado para AWS Glue que transforma los JSON crudos de SIMEM desde `bronze/simem-data` hacia una capa `silver` en `parquet` y luego construye una capa `gold` para modelado.

## Arquitectura recomendada

1. `s3://<bucket>/bronze/simem-data/...`
   Aqui vive el JSON crudo que carga el archiver Node.js.
2. Glue Job `simem-bronze-to-silver`
   Lee los JSON por dataset, los normaliza y agrega metadata del archivo fuente.
3. `s3://<bucket>/silver/simem-data/<dataset_slug>/year=YYYY/month=MM/`
   Aqui queda el `parquet` particionado para consulta.
4. Glue Crawler sobre `silver/simem-data/`
   Cataloga las tablas para Athena o consumo posterior.
5. Glue Job `simem-silver-to-gold-features`
   Agrega los datasets horarios y diarios en una sola tabla de features para forecasting.
6. `s3://<bucket>/gold/simem-features/demanda-real-hourly/`
   Aqui queda la capa `gold` en `parquet`.
7. Baseline local
   Valida nulos, granularidad, split temporal y un modelo simple antes de pasar a SageMaker.

## Job incluido

Script: [jobs/simem_bronze_to_silver.py](/Users/santiagomolano/thinklp/simem-glue-pipeline/jobs/simem_bronze_to_silver.py)

Hace esto:

- Lee todos los archivos `json` del prefijo `bronze/simem-data`.
- Detecta el `dataset_slug` desde el nombre del archivo.
- Sanitiza nombres de columnas a `snake_case` ASCII.
- Agrega columnas:
  - `dataset_slug`
  - `source_file`
  - `period_start`
  - `period_end`
  - `processed_at`
  - `year`
  - `month`
- Escribe `parquet` en `silver/simem-data/<dataset_slug>/year=.../month=.../`

## Job Gold incluido

Script: [jobs/simem_silver_to_gold_features.py](/Users/santiagomolano/thinklp/simem-glue-pipeline/jobs/simem_silver_to_gold_features.py)

Hace esto:

- Lee `demanda-real`, `demanda-comercial`, `generacion-real`, `aporte-hidricos` y `unidades-generacion` desde `silver`.
- Agrega series horarias y diarias a una granularidad horaria.
- Usa `demanda_real` como target por defecto.
- Crea features de calendario, lags y rolling windows.
- Genera una etiqueta futura configurable con `--LABEL_HORIZON_HOURS`.
- Escribe `parquet` en `gold/simem-features/demanda-real-hourly/`.

## Parametros del Glue Job

- `--JOB_NAME`: nombre del job de Glue.
- `--SOURCE_PATH`: ruta S3 origen, por ejemplo `s3://eafit-proyecto-integrador-simem/bronze/simem-data/`
- `--TARGET_PATH`: ruta S3 destino, por ejemplo `s3://eafit-proyecto-integrador-simem/silver/simem-data/`
- `--DATASET_SLUGS`: opcional. Lista separada por comas para procesar solo algunos datasets, por ejemplo `generacion-real,demanda-real`

## GitHub Actions

Quedo listo para ejecutarse desde GitHub Actions con estos workflows:

- [.github/workflows/deploy-glue.yml](/Users/santiagomolano/thinklp/simem-glue-pipeline/.github/workflows/deploy-glue.yml)
- [.github/workflows/run-glue-job.yml](/Users/santiagomolano/thinklp/simem-glue-pipeline/.github/workflows/run-glue-job.yml)
- [.github/workflows/deploy-gold-glue.yml](/Users/santiagomolano/thinklp/simem-glue-pipeline/.github/workflows/deploy-gold-glue.yml)
- [.github/workflows/run-gold-glue-job.yml](/Users/santiagomolano/thinklp/simem-glue-pipeline/.github/workflows/run-gold-glue-job.yml)

### Variables y secretos del repo

Variables recomendadas:

- `AWS_REGION_NAME=us-east-1`
- `S3_BUCKET_NAME=eafit-proyecto-integrador-simem`
- `GLUE_ROLE_NAME=simem-glue-service-role`
- `GLUE_DATABASE_NAME=simem_silver`
- `GLUE_JOB_NAME=simem-bronze-to-silver`
- `GLUE_CRAWLER_NAME=simem-silver-crawler`
- `GLUE_GOLD_DATABASE_NAME=simem_gold`
- `GLUE_GOLD_JOB_NAME=simem-silver-to-gold-features`
- `GLUE_GOLD_CRAWLER_NAME=simem-gold-features-crawler`

Secreto requerido:

- `AWS_GITHUB_ACTIONS_ROLE_ARN`

Ese secreto debe apuntar a un role de AWS asumible via GitHub OIDC y con permisos para:

- actualizar o crear Glue jobs y crawlers
- leer y escribir en el bucket objetivo
- usar el Data Catalog
- pasar el role de Glue si el workflow tambien administra el job

## Flujo de despliegue sugerido

1. Despliegue automatizado:

```bash
bash /Users/santiagomolano/thinklp/simem-glue-pipeline/scripts/deploy_glue_resources.sh
```

2. Sube el script del job a S3 manualmente si lo prefieres:

```bash
aws s3 cp \
  /Users/santiagomolano/thinklp/simem-glue-pipeline/jobs/simem_bronze_to_silver.py \
  s3://eafit-proyecto-integrador-simem/glue/scripts/simem_bronze_to_silver.py \
  --profile 878311411214_AdministratorAccess
```

3. Crea o reutiliza un role de Glue con permisos sobre:
   - `s3://eafit-proyecto-integrador-simem/bronze/*`
   - `s3://eafit-proyecto-integrador-simem/silver/*`
   - CloudWatch Logs
   - Glue Data Catalog

4. Crea el job en Glue Studio o por CLI. Valores recomendados:
   - Runtime: Glue 4.0
   - Language: Python
   - Worker type: `G.1X`
   - Number of workers: `2`
   - Job bookmark: `Disable`

5. Ejecuta el job con argumentos:

```text
--SOURCE_PATH=s3://eafit-proyecto-integrador-simem/bronze/simem-data/
--TARGET_PATH=s3://eafit-proyecto-integrador-simem/silver/simem-data/
```

Cuando ejecutes `scripts/run_glue_job.sh` para el job `simem-bronze-to-silver`, el crawler `simem-silver-crawler` se inicia por defecto si `START_CRAWLER` no esta definido. Si quieres omitirlo en una corrida puntual, usa `START_CRAWLER=false`.

6. Crea un crawler sobre:

```text
s3://eafit-proyecto-integrador-simem/silver/simem-data/
```

Database sugerida:

```text
simem_silver
```

## Flujo Gold sugerido

1. Despliega los recursos Gold:

```bash
bash /Users/santiagomolano/thinklp/simem-glue-pipeline/scripts/deploy_gold_resources.sh
```

2. Ejecuta el job Gold con horizonte de 24 horas:

```bash
EXTRA_GLUE_ARGS='{"--LABEL_HORIZON_HOURS":"24"}' \
GLUE_JOB_NAME=simem-silver-to-gold-features \
GLUE_CRAWLER_NAME=simem-gold-features-crawler \
bash /Users/santiagomolano/thinklp/simem-glue-pipeline/scripts/run_glue_job.sh
```

3. Crea o actualiza el crawler sobre:

```text
s3://eafit-proyecto-integrador-simem/gold/simem-features/demanda-real-hourly/
```

Database sugerida:

```text
simem_gold
```

## Baseline local

Despues de construir `gold`, revisa [baseline/README.md](/Users/santiagomolano/thinklp/simem-glue-pipeline/baseline/README.md).

## QuickSight como codigo

Para crear o actualizar el data source de Athena y los datasets base de QuickSight a partir del schema que ya existe en Glue, usa este script:

```bash
python scripts/sync_quicksight_resources.py
```

Por defecto sincroniza todas las tablas de:

- `simem_refined`
- `simem_gold`

Y crea o actualiza:

- data source `athena-simem-primary`
- un dataset por cada tabla encontrada
- ingestas `SPICE`

Ejemplos utiles:

Solo ver que haria:

```bash
python scripts/sync_quicksight_resources.py --dry-run
```

Sincronizar solo tablas puntuales:

```bash
python scripts/sync_quicksight_resources.py \
  --table simem_refined.eda_demanda_mensual \
  --table simem_refined.eda_demanda_real_vs_comercial \
  --table simem_gold.demanda_real_hourly
```

Esperar a que terminen las ingestas SPICE:

```bash
python scripts/sync_quicksight_resources.py --wait-for-ingestions
```

Otorgar al role de servicio de QuickSight acceso de lectura al bucket del proyecto y luego sincronizar datasets:

```bash
python scripts/sync_quicksight_resources.py \
  --grant-s3-bucket-access eafit-proyecto-integrador-simem \
  --wait-for-ingestions
```

Notas:

- El script lee tipos y columnas desde `Glue`, asi que no toca mantener schemas a mano.
- Si QuickSight todavia no tiene autorizados `Athena` y el bucket `S3` desde `Manage QuickSight -> Security & permissions`, la creacion puede fallar y esa habilitacion toca hacerla una vez.
- La opcion `--grant-s3-bucket-access` agrega una policy inline de lectura al role `aws-quicksight-service-role-v0`, que suele ser suficiente cuando las ingestas SPICE fallan por acceso al bucket.
- El workgroup usado por defecto es `primary`, pero puedes cambiarlo con `--workgroup`.

## Validacion local

Puedes validar la sintaxis del script con:

```bash
python3 -m py_compile /Users/santiagomolano/thinklp/simem-glue-pipeline/jobs/simem_bronze_to_silver.py
```

## Notas operativas

- El job escribe con `mode("overwrite")` y `partitionOverwriteMode=dynamic`, asi que si vuelves a correrlo, reemplaza solo las particiones tocadas.
- Como cada dataset puede tener schema distinto, el job escribe una carpeta separada por `dataset_slug`.
- Despues del job, lo mejor es consultar Athena contra la capa `silver`, no contra `bronze`.

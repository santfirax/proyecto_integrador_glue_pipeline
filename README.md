# SIMEM Glue Pipeline

Proyecto separado para AWS Glue que transforma los JSON crudos de SIMEM desde `bronze/simem-data` hacia una capa `silver` en `parquet`.

## Arquitectura recomendada

1. `s3://<bucket>/bronze/simem-data/...`
   Aqui vive el JSON crudo que carga el archiver Node.js.
2. Glue Job `simem-bronze-to-silver`
   Lee los JSON por dataset, los normaliza y agrega metadata del archivo fuente.
3. `s3://<bucket>/silver/simem-data/<dataset_slug>/year=YYYY/month=MM/`
   Aqui queda el `parquet` particionado para consulta.
4. Glue Crawler sobre `silver/simem-data/`
   Cataloga las tablas para Athena o consumo posterior.

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

## Parametros del Glue Job

- `--JOB_NAME`: nombre del job de Glue.
- `--SOURCE_PATH`: ruta S3 origen, por ejemplo `s3://eafit-proyecto-integrador-simem/bronze/simem-data/`
- `--TARGET_PATH`: ruta S3 destino, por ejemplo `s3://eafit-proyecto-integrador-simem/silver/simem-data/`
- `--DATASET_SLUGS`: opcional. Lista separada por comas para procesar solo algunos datasets, por ejemplo `generacion-real,demanda-real`

## GitHub Actions

Quedo listo para ejecutarse desde GitHub Actions con estos workflows:

- [.github/workflows/deploy-glue.yml](/Users/santiagomolano/thinklp/simem-glue-pipeline/.github/workflows/deploy-glue.yml)
- [.github/workflows/run-glue-job.yml](/Users/santiagomolano/thinklp/simem-glue-pipeline/.github/workflows/run-glue-job.yml)

### Variables y secretos del repo

Variables recomendadas:

- `AWS_REGION_NAME=us-east-1`
- `S3_BUCKET_NAME=eafit-proyecto-integrador-simem`
- `GLUE_ROLE_NAME=simem-glue-service-role`
- `GLUE_DATABASE_NAME=simem_silver`
- `GLUE_JOB_NAME=simem-bronze-to-silver`
- `GLUE_CRAWLER_NAME=simem-silver-crawler`

Secreto recomendado:

- `AWS_GITHUB_ACTIONS_ROLE_ARN`

Ese secreto debe apuntar a un role de AWS asumible via GitHub OIDC y con permisos para:

- actualizar o crear Glue jobs y crawlers
- leer y escribir en el bucket objetivo
- usar el Data Catalog
- pasar el role de Glue si el workflow tambien administra el job

### Bootstrap sin OIDC

Si todavia no tienes un role OIDC para GitHub Actions, los workflows tambien soportan estos secretos temporales:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`

Con esos tres secretos puedes ejecutar el pipeline una primera vez para crear:

- el role de Glue
- la database de Glue
- el job
- el crawler

Luego, idealmente migras a OIDC y dejas de depender de credenciales temporales.

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

6. Crea un crawler sobre:

```text
s3://eafit-proyecto-integrador-simem/silver/simem-data/
```

Database sugerida:

```text
simem_silver
```

## Validacion local

Puedes validar la sintaxis del script con:

```bash
python3 -m py_compile /Users/santiagomolano/thinklp/simem-glue-pipeline/jobs/simem_bronze_to_silver.py
```

## Notas operativas

- El job escribe con `mode("overwrite")` y `partitionOverwriteMode=dynamic`, asi que si vuelves a correrlo, reemplaza solo las particiones tocadas.
- Como cada dataset puede tener schema distinto, el job escribe una carpeta separada por `dataset_slug`.
- Despues del job, lo mejor es consultar Athena contra la capa `silver`, no contra `bronze`.

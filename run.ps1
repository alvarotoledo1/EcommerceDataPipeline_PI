<#
.SYNOPSIS
    Atajos para operar el pipeline. Todo pasa por docker compose.

.EXAMPLE
    .\run.ps1 quickstart    # TODO de una: build + up + pipeline completo
    .\run.ps1 build         # construye las tres imágenes
    .\run.ps1 up            # levanta MinIO y Airflow
    .\run.ps1 pipeline      # Bronze -> Silver con PySpark
    .\run.ps1 gold          # Silver -> Gold con dbt
    .\run.ps1 validate      # reconcilia las tres capas
    .\run.ps1 all           # pipeline + gold + validate
    .\run.ps1 test          # tests de pytest
    .\run.ps1 dag           # dispara el DAG completo en Airflow
    .\run.ps1 down          # apaga los servicios (conserva los datos)
    .\run.ps1 reset         # apaga y BORRA los volúmenes
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "up", "down", "reset", "pipeline", "gold", "validate",
                 "all", "test", "dag", "logs", "status", "shell", "quickstart")]
    [string]$Comando = "status"
)

# "Continue" y no "Stop": docker escribe su progreso en stderr, y con "Stop" PowerShell
# lo trataria como un error fatal. El control real es $LASTEXITCODE en Invocar.
$ErrorActionPreference = "Continue"

function Invocar($descripcion, [scriptblock]$bloque) {
    Write-Host "==> $descripcion" -ForegroundColor Cyan
    & $bloque
    if ($LASTEXITCODE -ne 0) {
        Write-Host "==> FALLÓ: $descripcion (código $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

switch ($Comando) {
    "quickstart" {
        # Equivalente de quickstart.sh para quien no tenga bash a mano.
        Invocar "Construyendo imágenes" { docker compose build }
        Invocar "Levantando MinIO y Airflow" { docker compose up -d minio airflow }
        Invocar "Bronze -> Silver" {
            docker compose run --rm spark python -m jobs.run_pipeline
        }
        Invocar "Silver -> Gold" { docker compose run --rm dbt dbt build }
        Invocar "Validando el flujo Medallion" {
            docker compose run --rm spark python -m jobs.validate_medallion
        }
        Write-Host ""
        Write-Host "  LISTO. Resultado final:" -ForegroundColor Green
        Write-Host "    output\daily_product_sales.parquet"
        Write-Host "    output\daily_product_sales.csv"
        Write-Host "    output\reports\*.json   (validaciones)"
        Write-Host ""
        Write-Host "  MinIO   http://localhost:9001  (minioadmin / minioadmin)"
        Write-Host "  Airflow http://localhost:8080  (admin / admin)"
    }
    "build" {
        Invocar "Construyendo imágenes (spark, dbt, airflow)" { docker compose build }
    }
    "up" {
        Invocar "Levantando MinIO y Airflow" { docker compose up -d minio airflow }
        Write-Host ""
        Write-Host "  MinIO   http://localhost:9001  (minioadmin / minioadmin)"
        Write-Host "  Airflow http://localhost:8080  (admin / admin)"
    }
    "down"  { Invocar "Apagando servicios" { docker compose down } }
    "reset" { Invocar "Apagando y borrando volúmenes" { docker compose down -v } }

    "pipeline" {
        Invocar "Bronze -> Silver" {
            docker compose run --rm spark python -m jobs.run_pipeline
        }
    }
    "gold" {
        Invocar "Silver -> Gold" { docker compose run --rm dbt dbt build }
    }
    "validate" {
        Invocar "Validando el flujo Medallion" {
            docker compose run --rm spark python -m jobs.validate_medallion
        }
    }
    "all" {
        Invocar "Bronze -> Silver" {
            docker compose run --rm spark python -m jobs.run_pipeline
        }
        Invocar "Silver -> Gold" { docker compose run --rm dbt dbt build }
        Invocar "Validando el flujo Medallion" {
            docker compose run --rm spark python -m jobs.validate_medallion
        }
    }
    "test"  { Invocar "pytest" { docker compose run --rm spark pytest } }
    "dag"   {
        Invocar "Ejecutando el DAG completo" {
            docker compose exec -T airflow airflow dags test olist_medallion
        }
    }
    "logs"  { docker compose logs -f --tail 100 }
    "shell" { docker compose run --rm spark bash }
    "status" {
        docker compose ps
        Write-Host ""
        Write-Host "Comandos: build | up | pipeline | gold | validate | all | test | dag | logs | shell | down | reset"
    }
}

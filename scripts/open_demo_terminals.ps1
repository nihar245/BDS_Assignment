# ============================================================
# BDS Uber Pipeline - Open 6 Demo Terminals
# ============================================================

$ROOT = "D:\BDS_robust_dashboard"

Write-Host "============================================"
Write-Host " BDS Uber Streaming Pipeline"
Write-Host "============================================"
Write-Host "Project: $ROOT"
Write-Host ""

if (-not (Test-Path $ROOT)) {
    Write-Host "ERROR: Project directory not found!" -ForegroundColor Red
    exit 1
}

# ------------------------------------------------------------
# Common environment setup
# ------------------------------------------------------------

$COMMON = @"
Set-Location '$ROOT'

# Initialize Conda
conda activate bds-uber

# Load .env
if (Test-Path '.env') {
    Get-Content '.env' | ForEach-Object {
        `$line = `$_.Trim()

        if (`$line -and -not `$line.StartsWith('#') -and `$line.Contains('=')) {
            `$parts = `$line.Split('=', 2)
            `$key = `$parts[0].Trim()
            `$value = `$parts[1].Trim()

            if (
                (`$value.StartsWith('"') -and `$value.EndsWith('"')) -or
                (`$value.StartsWith("'") -and `$value.EndsWith("'"))
            ) {
                `$value = `$value.Substring(1, `$value.Length - 2)
            }

            Set-Item -Path "Env:`$key" -Value `$value
        }
    }
}

Write-Host ""
Write-Host "Environment loaded." -ForegroundColor Green
Write-Host "Project: `$((Get-Location).Path)" -ForegroundColor Cyan
Write-Host "Python:  `$((Get-Command python).Source)" -ForegroundColor Cyan
Write-Host ""
"@

# ------------------------------------------------------------
# 1. HDFS
# ------------------------------------------------------------

$HDFS = $COMMON + @"

`$env:HADOOP_HOME = 'C:\hadoop-3.3.6'
`$env:JAVA_HOME = 'C:\Program Files\Java\jdk-8'
`$env:PATH = "`$env:HADOOP_HOME\bin;`$env:HADOOP_HOME\sbin;`$env:PATH"

Write-Host "Starting HDFS..." -ForegroundColor Cyan
Write-Host ""

& "`$env:HADOOP_HOME\sbin\start-dfs.cmd"

Write-Host ""
Write-Host "HDFS is running." -ForegroundColor Green
Write-Host ""
jps
Write-Host ""
Write-Host "This terminal is your HDFS terminal." -ForegroundColor Yellow
Write-Host ""

cmd /c pause
"@

# ------------------------------------------------------------
# 2. Kafka
# ------------------------------------------------------------

$KAFKA = $COMMON + @"

Set-Location '$ROOT\kafka'

Write-Host "Starting Kafka..." -ForegroundColor Cyan
Write-Host ""

docker compose up

"@

# ------------------------------------------------------------
# 3. Spark
# ------------------------------------------------------------

$SPARK = $COMMON + @"

`$env:SPARK_HOME = 'C:\spark-3.5.9-bin-hadoop3'
`$env:HADOOP_HOME = 'C:\hadoop-3.3.6'
`$env:JAVA_HOME = 'C:\Program Files\Java\jdk-8'

`$env:PATH = "`$env:SPARK_HOME\bin;`$env:HADOOP_HOME\bin;`$env:HADOOP_HOME\sbin;`$env:PATH"

`$env:PYSPARK_PYTHON = "`$env:CONDA_PREFIX\python.exe"
`$env:PYSPARK_DRIVER_PYTHON = "`$env:CONDA_PREFIX\python.exe"

Write-Host "============================================"
Write-Host " Spark Streaming Terminal"
Write-Host "============================================"
Write-Host ""
Write-Host "SPARK_HOME:"
Write-Host `$env:SPARK_HOME
Write-Host ""
Write-Host "HADOOP_HOME:"
Write-Host `$env:HADOOP_HOME
Write-Host ""
Write-Host "JAVA_HOME:"
Write-Host `$env:JAVA_HOME
Write-Host ""
Write-Host "PYSPARK_PYTHON:"
Write-Host `$env:PYSPARK_PYTHON
Write-Host ""

Write-Host "Spark terminal is READY." -ForegroundColor Green
Write-Host ""
Write-Host "Run the Spark job when instructed." -ForegroundColor Yellow
Write-Host ""

cmd /c pause
"@

# ------------------------------------------------------------
# 4. Producer
# ------------------------------------------------------------

$PRODUCER = $COMMON + @"

Write-Host "============================================"
Write-Host " Uber Kafka Producer"
Write-Host "============================================"
Write-Host ""

Write-Host "Producer terminal is READY." -ForegroundColor Green
Write-Host ""
Write-Host "The new producer will use:"
Write-Host "  Batch size : 10"
Write-Host "  Interval   : 10 seconds"
Write-Host "  Kafka      : localhost:9092"
Write-Host ""

Write-Host "Run the producer when instructed." -ForegroundColor Yellow
Write-Host ""

cmd /c pause
"@

# ------------------------------------------------------------
# 5. PostgreSQL
# ------------------------------------------------------------

$POSTGRES = $COMMON + @"

Write-Host "============================================"
Write-Host " PostgreSQL"
Write-Host "============================================"
Write-Host ""

`$PSQL = 'C:\Program Files\PostgreSQL\17\bin\psql.exe'

if (-not (Test-Path `$PSQL)) {
    Write-Host "ERROR: PostgreSQL psql.exe not found:" -ForegroundColor Red
    Write-Host `$PSQL
    cmd /c pause
    exit
}

Write-Host "Connecting to uber_streaming..." -ForegroundColor Cyan
Write-Host ""

& `$PSQL -h localhost -p 5432 -U postgres -d uber_streaming

"@

# ------------------------------------------------------------
# 6. Airflow
# ------------------------------------------------------------

$AIRFLOW = $COMMON + @"

Set-Location '$ROOT\airflow'

Write-Host "Starting Airflow..." -ForegroundColor Cyan
Write-Host ""

docker compose --env-file "$ROOT\.env" up

"@

# ------------------------------------------------------------
# Function to open PowerShell terminal
# ------------------------------------------------------------

function Open-BDSTerminal {
    param(
        [string]$Title,
        [string]$Command
    )

    $encodedCommand = [Convert]::ToBase64String(
        [System.Text.Encoding]::Unicode.GetBytes($Command)
    )

    Start-Process powershell.exe `
        -ArgumentList @(
            "-NoExit",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            $encodedCommand
        )
}

# ------------------------------------------------------------
# Open terminals
# ------------------------------------------------------------

Write-Host "Opening 6 terminals..." -ForegroundColor Cyan
Write-Host ""

Open-BDSTerminal "BDS - HDFS"       $HDFS
Start-Sleep -Seconds 1

Open-BDSTerminal "BDS - Kafka"      $KAFKA
Start-Sleep -Seconds 1

Open-BDSTerminal "BDS - Spark"      $SPARK
Start-Sleep -Seconds 1

Open-BDSTerminal "BDS - Producer"   $PRODUCER
Start-Sleep -Seconds 1

Open-BDSTerminal "BDS - PostgreSQL" $POSTGRES
Start-Sleep -Seconds 1

Open-BDSTerminal "BDS - Airflow"    $AIRFLOW

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " 6 terminals opened successfully." -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
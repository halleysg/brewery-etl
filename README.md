# Brewery Data Pipeline

A ETL pipeline for processing Open Brewery DB API data using Apache Airflow, PySpark, and a medallion architecture (Bronze → Silver → Gold layers).

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- 4GB RAM minimum
- 8GB free disk space

### Start the Pipeline

1. **Clone the repository**
   ```bash
   git clone https://github.com/halleysg/brewery-etl.git
   cd brewery-etl
   ```

2. **Build the Docker images**
   ```bash
   docker compose build
   ```

3. **Start the services**
   ```bash
   docker compose up -d
   ```

3. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

4. **Activate the DAG**
   - In Airflow UI, find `brewery_etl_pipeline`
   - Toggle the DAG to "ON"
   - Click "Trigger DAG" to run manually

## 📁 Project Structure

```
brewery-pipeline/
├── compose.yml                       # Container orchestration
├── Dockerfile                        # Airflow + PySpark image
├── requirements.txt                  # Python dependencies
├── airflow/
│   └── dags/           
│      └── brewery_pipeline.py        # Main ETL DAG
├── spark/
│   └── jobs/   
│      ├── fetch_brewery_data.py      # Fetch API data 
│      ├── brewery_medallion_etl.py/  # Main ETL task
│   └── data/   
│      ├── raw/                       # Raw data landing zone
│      ├── bronze/                    # Raw data ingestion
│      ├── silver/                    # Cleaned/validated data
│      └── gold/                      # Aggregated analytics data
└── logs/                             # Airflow logs
```

## 🔄 Pipeline Architecture

### Data Flow
```
API Fetch → Bronze Layer → Silver Layer → Gold Layer
```

### Layers Description

| Layer | Purpose | Data Format | Partitioning |
|-------|---------|-------------|--------------|
| **Bronze** | Raw data storage | Parquet | By load_date |
| **Silver** | Cleaned & standardized | Parquet | By brewery_type |
| **Gold** | Business aggregations | Parquet | - |

## 📊 Data Processing

### Preprocessing
- Fetches data from Open Brewery DB API
- Stores raw data as JSON file

### Bronze Layer
- Stores raw JSON as Parquet files
- Adds ingestion timestamp

### Silver Layer
- Data cleansing and standardization
- Type conversions and validation
- Data quality flag
- Deduplication

### Gold Layer
- Aggregation by type and location

## 🛠️ Configuration

### Airflow Configuration
- **Schedule**: Daily
- **Retries**: 1
- **Retry Delay**: 5 minutes
- **Catchup**: Disabled

## 📝 Usage Examples

### Check Pipeline Status
```bash
# View DAG runs
airflow dags list-runs -d brewery_etl_pipeline

# List tasks
airflow tasks list brewery_etl_pipeline
```

### Query Processed Data
```python
# Connect to data
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("QueryBrewery").getOrCreate()

# Read silver data
df = spark.read.parquet("./data/silver/breweries")
df.show()

# Read gold aggregations
agg_df = spark.read.parquet("./data/gold/brewery_summary")
agg_df.show()
```

## 🔍 Monitoring

### Airflow UI Features
- **DAG View**: Visual representation of pipeline
- **Task Logs**: Detailed execution logs
- **Task Duration**: Performance metrics
- **XCom**: Inter-task data passing

## 📋 API Reference

### Data Schema

**Silver Layer Schema:**
```
root
 |-- id: string
 |-- name: string (cleaned)
 |-- brewery_type: string (lowercase)
 |-- city: string (title case)
 |-- state: string (title case)
 |-- country: string (title case)
 |-- postal_code: int (digits only)
 |-- latitude: decimal
 |-- longitude: decimal
 |-- phone: bigint (digits only)
 |-- website_url: string
 |-- ingestion_timestamp: timestamp
 |-- valid_record: boolean
```

---

**Last Updated**: June 2025
**Version**: 1.0.1
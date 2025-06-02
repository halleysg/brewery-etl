from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pyspark.sql.functions import current_timestamp, lit, col, trim, regexp_replace, when, initcap, lower, concat_ws, nvl
import os
import sys

def create_spark_session():
    """Create and return Spark session"""
    print("Creating Spark session")

    spark = SparkSession.builder \
        .appName("BreweryMedallionETL") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    print(f"Spark version: {spark.version}")
    return spark

def check_raw_data(ds_nodash):
    """Verify raw data was fetched successfully"""
    
    raw_path = "/spark/data/raw/breweries"
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw data directory not found: {raw_path}")

    json_files = [f for f in os.listdir(raw_path) if f.startswith(f"breweries_{ds_nodash}T") and f.endswith(".json")]
    if not json_files:
        raise FileNotFoundError("No JSON files found in raw data directory")
    
    latest_file = sorted(json_files)[-1]
    file_path = os.path.join(raw_path, latest_file)
    file_size = os.path.getsize(file_path)
    
    print(f"✓ Found raw data file: {latest_file}")
    
    # Size validation
    if file_size < 1000:
        raise ValueError(f"Raw data file seems too small: {file_size} bytes")
    
    return file_path

def process_bronze_layer(spark, raw_json_path, ds_nodash):
    """Bronze Layer: Raw data ingestion"""
    
    print("\nProcessing Bronze Layer")
    
    # Check if file exists
    if not os.path.exists(raw_json_path):
        raise FileNotFoundError(f"File not found: {raw_json_path}")
    
    # Read raw JSON data
    df_raw = spark.read.option("multiline", "true").json(raw_json_path)
    
    if df_raw.isEmpty():
        raise ValueError(f"Empty Dataframe: No data to process")
    
    # Cast all columns to string
    df_bronze = df_raw.select([col(c).cast(StringType()).alias(c) for c in df_raw.columns])
    
    # Add bronze layer metadata
    df_bronze = df_bronze.withColumn("execution_ds", lit(ds_nodash)).withColumn("ingestion_timestamp", current_timestamp())
    
    bronze_path = "/spark/data/bronze/breweries"
    
    # Save Bronze layer as Parquet
    df_bronze.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("execution_ds").parquet(bronze_path)
    
    print(f"✓ Bronze layer saved")
    print(f"  Records: {df_bronze.count()}")
    
    return bronze_path

def process_silver_layer(spark, bronze_path, ds_nodash):
    """Silver Layer: Cleaned and standardized data"""
    
    print("\nProcessing Silver Layer")
    
    # Read Bronze data
    df_bronze = spark.read.parquet(f"{bronze_path}/execution_ds={ds_nodash}")
    
    # Clean and standardize
    df_silver = (df_bronze
        .dropna(subset=["id", "brewery_type"])
        .dropDuplicates(["id"])
        .select(
            "id",
            trim(col("name")).alias("name"),
            lower(trim(col("brewery_type"))).alias("brewery_type"),                                    # Standardize
            initcap(trim(col("city"))).alias("city"),                                                  # Capitalizing cities
            initcap(trim(nvl(col("state"), col("state_province")))).alias("state"),                    # Capitalizing states
            initcap(trim(col("country"))).alias("country"),                                            # Capitalizing country
            regexp_replace(col("postal_code"), "[^0-9]", "").cast(IntegerType()).alias("postal_code"), # Only numbers
            regexp_replace(col("phone"), "[^0-9]", "").cast(LongType()).alias("phone"),                # Only numbers
            concat_ws(", ",
                nvl(col("address_1"), col("street")),
                col("address_2"),
                col("address_3")
            ).alias("full_address"),
            col("latitude").cast(DecimalType(10,8)),  # Cast to decimal
            col("longitude").cast(DecimalType(11,8)), # Cast to decimal
            "website_url",
            current_timestamp().alias("ingestion_timestamp"),
            when(
                (col("name").isNotNull()) &
                (col("city").isNotNull()) &
                (col("state").isNotNull() | col("state_province").isNotNull()) &
                (col("country").isNotNull()), True
            ).otherwise(False).alias("valid_record") # Create Full Adress
        )
    )

    # Save Silver layer
    silver_path = "/spark/data/silver/breweries"
    
    df_silver.write.mode("overwrite").partitionBy("brewery_type").parquet(silver_path)
    
    record_count = df_silver.count()
    complete_count = df_silver.filter(col('valid_record')).count()
    
    print(f"✓ Silver layer saved")
    print(f"  Records: {record_count}")
    print(f"  Complete records: {complete_count}")
    
    return silver_path

def process_gold_layer(spark, silver_path):
    """Gold Layer: Business-ready aggregations and analytics"""
    print("\nProcessing Gold Layer")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path).filter(col("valid_record"))
    
    # 1. Brewery summary by location and type
    df_summary = (df_silver
        .groupBy("country", "state", "city", "brewery_type")
        .count().alias("brewery_count")
        .orderBy("country", "state", "city", "brewery_type")
    )

    gold_path = "/spark/data/gold/breweries"
    
    # Save Gold layer
    df_summary.coalesce(1).write.mode("overwrite").parquet(gold_path)
    
    # Display results
    print("\n📊 Breweries Summary:")
    df_summary.show(10, truncate=False)

    print(f"\n✓ Gold layer saved")

def brewery_medallion_etl(**context):
    """Main ETL pipeline"""    
    spark = None

    try:
        print("\n" + "=" * 50)
        print("STARTING ETL PIPELINE")
        print("=" * 50)
        
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        raw_file_path = check_raw_data(context['ds_nodash'])
        print(f"Processing file")
        
        # Process through medallion layers
        bronze_path = process_bronze_layer(spark, raw_file_path, context['ds_nodash'])
        silver_path = process_silver_layer(spark, bronze_path, context['ds_nodash'])
        process_gold_layer(spark, silver_path)
        
        print("\n" + "=" * 50)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nETL PIPELINE FAILED: {str(e)}", file=sys.stderr)
        raise
    finally:
        if spark:
            spark.stop()
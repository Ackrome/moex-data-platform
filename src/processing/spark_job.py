# File: src/processing/spark_job.py
import uuid
import socket
import psycopg2
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.config import settings

def get_spark_session(app_name: str = "MOEX_ETL_Strict"):
    container_ip = socket.gethostbyname(socket.gethostname())
    return (SparkSession.builder
        .appName(app_name)
        .master(settings.SPARK_MASTER_URL)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Optimization & Memory
        .config("spark.driver.memory", "1g") 
        .config("spark.executor.memory", "1g")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", container_ip)
        .getOrCreate())

def get_pg_connection():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST, port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER, password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DB
    )

def process_bronze_to_silver(spark, target_tickers: List[str] = None):
    print(f"🚀 [STAGE 1] Bronze -> Silver (Targets: {target_tickers or 'ALL'})")
    raw_path_root = f"s3a://{settings.MINIO_BUCKET_RAW}"
    silver_path = f"s3a://{settings.MINIO_BUCKET_SILVER}/market_data"

    schema = StructType([
        StructField("begin", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("value", DoubleType(), True), 
        StructField("end", StringType(), True)
    ])

    try:
        df = spark.read.format("json") \
            .schema(schema) \
            .option("recursiveFileLookup", "true") \
            .option("pathGlobFilter", "*.json") \
            .load(raw_path_root)

        df = df.withColumn("file_path", F.input_file_name())
        df = df.withColumn("ticker", F.regexp_extract(F.col("file_path"), r"\/([A-Z0-9]{3,6})\/(1[dm])\/", 1))
        df = df.withColumn("interval_type", F.regexp_extract(F.col("file_path"), r"\/([A-Z0-9]{3,6})\/(1[dm])\/", 2))

        if target_tickers:
            df = df.filter(F.col("ticker").isin(target_tickers))

        if df.rdd.isEmpty():
            print("⚠️ [Bronze->Silver] No data found.")
            return

        df_clean = df.select(
            F.col("ticker"),
            F.col("interval_type").alias("interval"),
            F.to_timestamp(F.col("begin")).alias("ts"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume")
        ).dropna(subset=["ticker", "interval", "ts", "close"]) \
         .dropDuplicates(["ticker", "interval", "ts"])

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        print("💾 Saving to Silver Layer (Parquet)...")
        df_clean.write.mode("overwrite").partitionBy("ticker").parquet(silver_path)
        print("✅ Silver Layer Updated.")

    except Exception as e:
        print(f"❌ Error in Bronze->Silver: {e}")
        raise e

def process_silver_to_gold_atomic(spark, target_tickers: List[str] = None):
    print(f"🚀 [STAGE 2] Silver -> Gold (Atomic Swap) Targets: {target_tickers or 'ALL'}")
    silver_path = f"s3a://{settings.MINIO_BUCKET_SILVER}/market_data"
    
    try:
        df = spark.read.parquet(silver_path)
        
        if target_tickers:
            df = df.filter(F.col("ticker").isin(target_tickers))
        
        if df.rdd.isEmpty():
            print("⚠️ [Silver->Gold] No data found.")
            return

        # --- Calculations ---
        w = Window.partitionBy("ticker", "interval").orderBy("ts")
        df_indicators = df.withColumn("sma_20", F.avg("close").over(w.rowsBetween(-19, 0)))
        
        change = F.col("close") - F.lag("close", 1).over(w)
        gain = F.when(change > 0, change).otherwise(0)
        loss = F.when(change < 0, F.abs(change)).otherwise(0)
        avg_gain = F.avg(gain).over(w.rowsBetween(-13, 0))
        avg_loss = F.avg(loss).over(w.rowsBetween(-13, 0))
        rs = avg_gain / avg_loss
        rsi_calc = 100 - (100 / (1 + rs))
        rsi_safe = F.when(avg_loss == 0, 100.0).otherwise(rsi_calc)
        
        df_final = df_indicators.withColumn("rsi_14", F.coalesce(rsi_safe, F.lit(50.0)))
        
        # --- FIX: Convert Timestamp to String for Safe Transport ---
        # Postgres can cast string '2024-01-01 10:00:00' to Timestamp easily.
        # But it cannot cast Double (Epoch) to Timestamp implicitly.
        df_final = df_final.withColumn("ts_str", F.date_format(F.col("ts"), "yyyy-MM-dd HH:mm:ss"))
        # Drop original ts object to avoid confusion in JDBC
        df_final = df_final.drop("ts")

        row_count = df_final.count()
        print(f"✅ Calculated {row_count} rows.")
        if row_count == 0: return

        # --- STAGING WRITE ---
        run_id = str(uuid.uuid4()).replace("-", "")[:8]
        temp_table = f"temp_metrics_{run_id}"
        
        jdbc_url = f"jdbc:postgresql://{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        props = {"user": settings.POSTGRES_USER, "password": settings.POSTGRES_PASSWORD, "driver": "org.postgresql.Driver"}

        print(f"💾 Writing to STAGING table: {temp_table}...")
        df_final.write.jdbc(url=jdbc_url, table=temp_table, mode="overwrite", properties=props)
        
        # --- ATOMIC MERGE ---
        print("🔄 Performing ATOMIC MERGE in Postgres...")
        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                # 1. Start Transaction
                # Удаляем старые данные для этих тикеров
                if target_tickers:
                    cur.execute(f"DELETE FROM stock_metrics WHERE ticker = ANY(%s)", (target_tickers,))
                else:
                    cur.execute("TRUNCATE TABLE stock_metrics") # Full refresh case
                
                # 2. Insert from Temp with EXPLICIT CASTING
                # Мы берем строку ts_str и кастуем её в timestamp: ts_str::timestamp
                insert_query = f"""
                    INSERT INTO stock_metrics 
                    (ticker, interval, ts, open, high, low, close, volume, sma_20, rsi_14)
                    SELECT 
                        ticker, 
                        interval, 
                        ts_str::timestamp as ts, 
                        open, 
                        high, 
                        low, 
                        close, 
                        volume, 
                        sma_20, 
                        rsi_14 
                    FROM {temp_table}
                """
                cur.execute(insert_query)
                
                # 3. Cleanup
                cur.execute(f"DROP TABLE {temp_table}")
                
                conn.commit()
                print("✅ ATOMIC SWAP COMPLETED SUCCESSFULLY.")
                
        except Exception as e:
            conn.rollback()
            print(f"❌ Transaction FAILED. Rollback executed. Error: {e}")
            # Try to cleanup garbage
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    conn.commit()
            except: pass
            raise e
        finally:
            conn.close()

    except Exception as e:
        print(f"❌ Error in Silver->Gold: {e}")
        raise e

def process_data(tickers: List[str] = None):
    spark = get_spark_session()
    try:
        process_bronze_to_silver(spark, tickers)
        process_silver_to_gold_atomic(spark, tickers)
    finally:
        spark.stop()

if __name__ == "__main__":
    process_data(["SBER"])
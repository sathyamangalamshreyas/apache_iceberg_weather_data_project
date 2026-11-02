import logging
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    max,
    round,
    sum,
    to_date,
)

# --------------------------------------------------
# 1. Configure logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("WeatherDemo-Gold")


# --------------------------------------------------
# 2. Configuration
# --------------------------------------------------

SILVER = "local.weather_demo.silver_observations"
GOLD = "local.weather_demo.gold_daily_summary"

OUTPUT = Path("/opt/spark-data/weatherledger_project/powerbi/daily_weather_summary.csv")

spark = None

try:
    # --------------------------------------------------
    # 3. Initialize Spark session
    # --------------------------------------------------

    logger.info("Starting WeatherLedger Gold ETL job")

    spark = SparkSession.builder.appName("WeatherLedger-Gold").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Spark session initialized successfully")

    # --------------------------------------------------
    # 4. Read Silver and aggregate daily observations
    # --------------------------------------------------

    logger.info("Reading Silver table: %s", SILVER)

    daily = (
        spark.table(SILVER)
        .withColumn("observation_date", to_date("observed_at"))
        .groupBy("station_id", "observation_date")
        .agg(
            count("event_id").alias("observation_count"),
            round(avg("temperature_c"), 2).alias("avg_temperature_c"),
            round(avg("humidity_pct"), 2).alias("avg_humidity_pct"),
            round(sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
            round(max("wind_speed_kmh"), 2).alias("max_wind_speed_kmh"),
        )
    )

    daily.createOrReplaceTempView("daily_updates")

    logger.info("Daily weather aggregation completed")

    # --------------------------------------------------
    # 5. Create or incrementally update Gold table
    # --------------------------------------------------

    if not spark.catalog.tableExists(GOLD):
        logger.info("Gold table does not exist. Creating: %s", GOLD)

        (daily.writeTo(GOLD).using("iceberg").create())

        logger.info("Gold table created successfully")

    else:
        logger.info("Updating existing Gold table: %s", GOLD)

        spark.sql(f"""
            MERGE INTO {GOLD} AS t
            USING daily_updates AS s

            ON t.station_id = s.station_id
            AND t.observation_date = s.observation_date

            WHEN MATCHED AND (
                NOT (t.observation_count <=> s.observation_count)
                OR NOT (t.avg_temperature_c <=> s.avg_temperature_c)
                OR NOT (t.avg_humidity_pct <=> s.avg_humidity_pct)
                OR NOT (t.total_precipitation_mm <=> s.total_precipitation_mm)
                OR NOT (t.max_wind_speed_kmh <=> s.max_wind_speed_kmh)
            )

            THEN UPDATE SET
                t.observation_count = s.observation_count,
                t.avg_temperature_c = s.avg_temperature_c,
                t.avg_humidity_pct = s.avg_humidity_pct,
                t.total_precipitation_mm = s.total_precipitation_mm,
                t.max_wind_speed_kmh = s.max_wind_speed_kmh

            WHEN NOT MATCHED THEN
                INSERT (
                    station_id,
                    observation_date,
                    observation_count,
                    avg_temperature_c,
                    avg_humidity_pct,
                    total_precipitation_mm,
                    max_wind_speed_kmh
                )
                VALUES (
                    s.station_id,
                    s.observation_date,
                    s.observation_count,
                    s.avg_temperature_c,
                    s.avg_humidity_pct,
                    s.total_precipitation_mm,
                    s.max_wind_speed_kmh
                )
        """)

        logger.info("Gold table MERGE completed successfully")

    # --------------------------------------------------
    # 6. Export Gold table for Power BI
    # --------------------------------------------------

    logger.info("Preparing Power BI CSV export")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    gold_df = spark.table(GOLD)

    gold_count = gold_df.count()

    logger.info("Gold table contains %s records", f"{gold_count:,}")

    (
        gold_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("delimiter", ",")
        .csv(str(OUTPUT))
    )

    logger.info("Power BI CSV export completed: %s", OUTPUT)

    # --------------------------------------------------
    # 7. Execution summary
    # --------------------------------------------------

    logger.info("----------------------------------------")
    logger.info("WeatherLedger Gold ETL completed")
    logger.info("Gold table: %s", GOLD)
    logger.info("Gold records: %s", f"{gold_count:,}")
    logger.info("Power BI output: %s", OUTPUT)
    logger.info("----------------------------------------")


# --------------------------------------------------
# 8. Handle PySpark-specific exceptions
# --------------------------------------------------

except PySparkException as exc:
    logger.exception(
        "PySpark job failed. Error class: %s, Message parameters: %s",
        exc.getErrorClass(),
        exc.getMessageParameters(),
    )

    raise


# --------------------------------------------------
# 9. Handle other Python runtime exceptions
# --------------------------------------------------

except Exception:
    logger.exception("Unexpected error occurred during WeatherLedger Gold ETL")

    raise


# --------------------------------------------------
# 10. Always release Spark resources
# --------------------------------------------------

finally:
    if spark is not None:
        try:
            spark.stop()
            logger.info("Spark session stopped successfully")

        except Exception:
            logger.exception("Error while stopping Spark session")

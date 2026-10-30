# Insert Libraries
import logging

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession, Window
from pyspark.sql.column import Column
from pyspark.sql.functions import col, row_number

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------

BRONZE = "local.weather_demo.bronze_events"
SILVER = "local.weather_demo.silver_observations"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("WeatherDemo-Silver")

# -------------------------------------------------------------
# Spark Session
# -------------------------------------------------------------

spark = SparkSession.builder.appName("WeatherDemo-Silver").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    # -------------------------------------------------------------
    # 1. Define deduplication window
    # -------------------------------------------------------------

    revision: Column = col("revision")
    ingested: Column = col("ingested_at")
    event: Column = col("event_id")

    latest = Window.partitionBy("station_id", "observed_at").orderBy(
        Column.desc(revision),
        Column.desc(ingested),
        Column.desc(event),
    )

    # -------------------------------------------------------------
    # 2. Read Bronze and apply data-quality filters
    # -------------------------------------------------------------

    logger.info("Reading Bronze table: %s", BRONZE)

    bronze = spark.table(BRONZE)

    updates = (
        bronze.filter(
            col("station_id").isNotNull()
            & col("observed_at").isNotNull()
            & col("revision").isNotNull()
            & col("ingested_at").isNotNull()
            & col("event_id").isNotNull()
            & col("temperature_c").between(-80, 60)
            & col("humidity_pct").between(0, 100)
            & (col("precipitation_mm") >= 0)
            & col("wind_speed_kmh").between(0, 400)
        )
        .withColumn("rn", row_number().over(latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # -------------------------------------------------------------
    # 3. Register temporary view for MERGE
    # -------------------------------------------------------------

    updates.createOrReplaceTempView("updates")

    logger.info("Prepared de-duplicated Silver updates")

    # -------------------------------------------------------------
    # 4. Create Silver table if it doesn't exist
    # -------------------------------------------------------------

    if not spark.catalog.tableExists(SILVER):
        logger.info("Silver table does not exist. Creating: %s", SILVER)

        updates.writeTo(SILVER).using("iceberg").create()

        logger.info("Silver table created successfully")

    # -------------------------------------------------------------
    # 5. Merge updates into existing Silver table
    # -------------------------------------------------------------

    else:
        logger.info("Merging updates into Silver table: %s", SILVER)

        spark.sql(f"""
            MERGE INTO {SILVER} AS t
            USING updates as s

            ON t.station_id = s.station_id
            AND t.observed_at = s.observed_at

            WHEN MATCHED AND (
                s.revision > t.revision

                OR (
                        s.revision = t.revision
                    AND s.ingested_at > t.ingested_at
                )

                OR (
                        s.revision = t.revision
                    AND s.ingested_at = t.ingested_at
                    AND s.event_id > t.event_id
                )
            )

            THEN UPDATE SET
                event_id            = s.event_id,
                revision            = s.revision,
                ingested_at         = s.ingested_at,
                temperature_c       = s.temperature_c,
                humidity_pct        = s.humidity_pct,
                precipitation_mm    = s.precipitation_mm,
                wind_speed_kmh      = s.wind_speed_kmh,
                source_batch_id     = s.source_batch_id

            WHEN NOT MATCHED THEN INSERT (
                station_id,
                observed_at,
                event_id,
                revision,
                ingested_at,
                temperature_c,
                humidity_pct,
                precipitation_mm,
                wind_speed_kmh,
                source_batch_id
            )
            VALUES (
                s.station_id,
                s.observed_at,
                s.event_id,
                s.revision,
                s.ingested_at,
                s.temperature_c,
                s.humidity_pct,
                s.precipitation_mm,
                s.wind_speed_kmh,
                s.source_batch_id
            )
        """)

        logger.info("Silver MERGE completed successfully")

    # -------------------------------------------------------------
    # 6. Report Silver table statistics
    # -------------------------------------------------------------

    silver_count = spark.table(SILVER).count()

    logger.info(
        "Silver current observations: %s",
        f"{silver_count:,}",
    )

except PySparkException as exc:
    logger.exception(
        "Spark job failed. Error class: %s, Message parameters: %s",
        exc.getErrorClass(),
        exc.getMessageParameters(),
    )
    # Preserve the original exception and traceback
    raise

finally:
    # Always release Spark resources
    logger.info("Stopping Spark session")

    spark.stop()

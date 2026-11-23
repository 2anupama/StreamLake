"""PySpark Structured Streaming consumer for Debezium PostgreSQL events."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from streamlake.common.settings import Settings
from streamlake.ingestion.streaming.cdc import checkpoint_location, parse_cdc_message

logger = logging.getLogger("streamlake.streaming")
KAFKA_CONNECTOR_COORDINATE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def _cdc_schema():
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("kafka_timestamp_ms", LongType()),
            StructField("raw_key", StringType()),
            StructField("raw_payload", StringType()),
            StructField("operation", StringType()),
            StructField("before_json", StringType()),
            StructField("after_json", StringType()),
            StructField("source_json", StringType()),
            StructField("schema_json", StringType()),
            StructField("transaction_json", StringType()),
            StructField("source_name", StringType()),
            StructField("source_database", StringType()),
            StructField("source_schema", StringType()),
            StructField("source_table", StringType()),
            StructField("source_lsn", LongType()),
            StructField("event_timestamp_ms", LongType()),
            StructField("error_reason", StringType()),
            StructField("is_tombstone", BooleanType()),
            StructField("employee_id", StringType()),
            StructField("table", StringType()),
        ]
    )


def _parse_for_spark(value, key, topic, partition, offset, kafka_timestamp):
    timestamp_ms = None
    if kafka_timestamp is not None:
        timestamp_ms = int(kafka_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
    event = parse_cdc_message(
        value,
        key=key,
        topic=topic,
        partition=int(partition),
        offset=int(offset),
        kafka_timestamp_ms=timestamp_ms,
    )
    row = event.to_record()
    row["employee_id"] = event.employee_id
    row["table"] = event.table
    return row


def build_stream(spark, settings: Settings):
    """Create a recoverable Structured Streaming Kafka source and parsed rows."""
    from pyspark.sql import functions as F
    from pyspark.sql.functions import udf

    topics = ",".join(
        (
            settings.kafka_employee_topic,
            settings.kafka_dependent_topic,
            settings.kafka_policy_topic,
            settings.kafka_company_topic,
        )
    )
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", settings.cdc_max_offsets_per_trigger)
        .load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
        )
    )
    parse_udf = udf(_parse_for_spark, _cdc_schema())
    return raw_stream.withColumn(
        "parsed",
        parse_udf("value", "key", "topic", "partition", "offset", "kafka_timestamp"),
    ).select("kafka_timestamp", "value", "key", "parsed.*")


def start_consumer(settings: Settings | None = None, spark=None) -> None:
    """Start the Spark query; Spark master is supplied by spark-submit/runtime."""
    from pyspark.sql import SparkSession

    settings = settings or Settings()
    spark = spark or SparkSession.builder.appName("StreamLake-Debezium-CDC").getOrCreate()
    parsed_stream = build_stream(spark, settings)

    def process_batch(batch_df, spark_batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("Spark batch %s is empty", spark_batch_id)
            return

        batch_token = f"spark-{spark_batch_id:020d}"
        now = datetime.now(timezone.utc).isoformat()
        from pyspark.sql import functions as F

        enriched = (
            batch_df.withColumn("spark_batch_id", F.lit(batch_token))
            .withColumn("processing_timestamp", F.lit(now))
            .withColumn(
                "event_date",
                F.to_date(
                    F.timestamp_seconds(
                        F.coalesce("event_timestamp_ms", "kafka_timestamp_ms") / 1000
                    )
                ),
            )
            .withColumn("event_date", F.coalesce("event_date", F.current_date()))
        )
        bronze_batch_path = Path(settings.cdc_bronze_path) / f"batch_id={batch_token}"
        stage_batch_path = Path(settings.cdc_staging_path) / f"batch_id={batch_token}"

        (enriched.write.mode("overwrite").partitionBy("event_date", "source_table").parquet(str(bronze_batch_path)))
        enriched.write.mode("overwrite").parquet(str(stage_batch_path))

        invalid = enriched.where(F.col("error_reason").isNotNull())
        if not invalid.isEmpty():
            invalid.write.mode("overwrite").partitionBy("event_date", "source_table").parquet(
                str(Path(settings.cdc_quarantine_path) / f"batch_id={batch_token}")
            )
            dead_letters = invalid.select(
                F.col("raw_key").cast("binary").alias("key"),
                F.to_json(
                    F.struct(
                        "event_id",
                        "topic",
                        "partition",
                        "offset",
                        "raw_payload",
                        "error_reason",
                        "processing_timestamp",
                    )
                ).alias("value"),
            )
            (
                dead_letters.write.format("kafka")
                .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
                .option("topic", settings.kafka_dead_letter_topic)
                .save()
            )

        counts = _apply_duckdb_batch(Path(settings.cdc_duckdb_path), stage_batch_path, settings)
        logger.info(
            "Spark batch %s processed; input=%s deduplicated=%s applied=%s rejected=%s",
            batch_token,
            counts["input"],
            counts["deduplicated"],
            counts["applied"],
            counts["rejected"],
        )

    query = (
        parsed_stream.writeStream.queryName("streamlake-employee-cdc")
        .option("checkpointLocation", checkpoint_location(str(settings.cdc_checkpoint_path)))
        .foreachBatch(process_batch)
        .start()
    )
    logger.info(
        "Started Structured Streaming query %s using Spark Kafka connector %s",
        query.id,
        KAFKA_CONNECTOR_COORDINATE,
    )
    query.awaitTermination()


def _apply_duckdb_batch(database_path: Path, stage_path: Path, settings: Settings) -> dict[str, int]:
    """Deduplicate and upsert a Spark-written Parquet microbatch transactionally."""
    import duckdb

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    stage_glob = str(stage_path / "**" / "*.parquet").replace("'", "''")
    silver_path = Path(settings.cdc_silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_silver_path = silver_path.with_name(f".{silver_path.name}-{uuid.uuid4().hex}.tmp.parquet")
    try:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS cdc_event_ledger (
                event_id VARCHAR PRIMARY KEY,
                batch_id VARCHAR NOT NULL,
                topic VARCHAR NOT NULL,
                partition_id INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                operation VARCHAR,
                source_table VARCHAR,
                employee_id VARCHAR,
                source_lsn BIGINT,
                event_timestamp_ms BIGINT,
                raw_payload VARCHAR,
                error_reason VARCHAR,
                processed_at TIMESTAMP NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS employee_current (
                employee_id VARCHAR PRIMARY KEY,
                record_status VARCHAR NOT NULL,
                data_json JSON NOT NULL,
                last_operation VARCHAR NOT NULL,
                last_event_id VARCHAR NOT NULL,
                source_lsn BIGINT,
                event_timestamp_ms BIGINT,
                transaction_order BIGINT,
                partition_id INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                source_name VARCHAR,
                source_database VARCHAR,
                source_schema VARCHAR,
                source_table VARCHAR,
                updated_at TIMESTAMPTZ NOT NULL
            )"""
        )
        connection.execute(f"CREATE TEMP TABLE cdc_stage AS SELECT * FROM read_parquet('{stage_glob}')")
        input_count = connection.execute("SELECT count(*) FROM cdc_stage").fetchone()[0]
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """CREATE TEMP TABLE cdc_new AS
               SELECT stage.* FROM cdc_stage AS stage
               LEFT JOIN cdc_event_ledger AS ledger USING (event_id)
               WHERE ledger.event_id IS NULL"""
        )
        deduplicated = connection.execute("SELECT count(*) FROM cdc_new").fetchone()[0]
        connection.execute(
            """INSERT INTO cdc_event_ledger
               SELECT event_id, spark_batch_id, topic, "partition", "offset", operation,
                      source_table, employee_id, source_lsn, event_timestamp_ms,
                      raw_payload, error_reason, current_timestamp
               FROM cdc_new"""
        )
        connection.execute(
            """CREATE TEMP TABLE employee_batch AS
               SELECT * EXCLUDE (event_rank)
               FROM (
                   SELECT *,
                          try_cast(json_extract_string(transaction_json, '$.total_order') AS BIGINT) AS transaction_order,
                          row_number() OVER (
                              PARTITION BY employee_id
                              ORDER BY source_lsn DESC NULLS LAST,
                                       event_timestamp_ms DESC NULLS LAST,
                                       try_cast(json_extract_string(transaction_json, '$.total_order') AS BIGINT) DESC NULLS LAST,
                                       "partition" DESC,
                                       "offset" DESC
                          ) AS event_rank
                   FROM cdc_new
                   WHERE source_table = 'employees'
                     AND employee_id IS NOT NULL
                     AND operation IN ('c', 'u', 'd', 'r')
                     AND error_reason IS NULL
               )
               WHERE event_rank = 1"""
        )
        connection.execute(
            """INSERT INTO employee_current
               SELECT batch.employee_id,
                      CASE WHEN batch.operation = 'd' THEN 'deleted' ELSE 'active' END,
                      CASE WHEN batch.operation = 'd'
                           THEN coalesce(current.data_json, batch.before_json, batch.after_json, '{}')::JSON
                           ELSE json_merge_patch(
                               coalesce(current.data_json, batch.before_json, '{}')::JSON,
                               coalesce(batch.after_json, '{}')::JSON
                           )
                      END,
                      batch.operation,
                      batch.event_id,
                      batch.source_lsn,
                      batch.event_timestamp_ms,
                      batch.transaction_order,
                      batch."partition",
                      batch."offset",
                      batch.source_name,
                      batch.source_database,
                      batch.source_schema,
                      batch.source_table,
                      current_timestamp
               FROM employee_batch AS batch
               LEFT JOIN employee_current AS current USING (employee_id)
               ON CONFLICT (employee_id) DO UPDATE SET
                   record_status = excluded.record_status,
                   data_json = excluded.data_json,
                   last_operation = excluded.last_operation,
                   last_event_id = excluded.last_event_id,
                   source_lsn = excluded.source_lsn,
                   event_timestamp_ms = excluded.event_timestamp_ms,
                   transaction_order = excluded.transaction_order,
                   partition_id = excluded.partition_id,
                   kafka_offset = excluded.kafka_offset,
                   source_name = excluded.source_name,
                   source_database = excluded.source_database,
                   source_schema = excluded.source_schema,
                   source_table = excluded.source_table,
                   updated_at = excluded.updated_at
               WHERE coalesce(excluded.source_lsn, 0) > coalesce(employee_current.source_lsn, 0)
                  OR (coalesce(excluded.source_lsn, 0) = coalesce(employee_current.source_lsn, 0)
                      AND coalesce(excluded.event_timestamp_ms, 0) > coalesce(employee_current.event_timestamp_ms, 0))
                  OR (coalesce(excluded.source_lsn, 0) = coalesce(employee_current.source_lsn, 0)
                      AND coalesce(excluded.event_timestamp_ms, 0) = coalesce(employee_current.event_timestamp_ms, 0)
                      AND coalesce(excluded.transaction_order, 0) > coalesce(employee_current.transaction_order, 0))
                  OR (coalesce(excluded.source_lsn, 0) = coalesce(employee_current.source_lsn, 0)
                      AND coalesce(excluded.event_timestamp_ms, 0) = coalesce(employee_current.event_timestamp_ms, 0)
                      AND coalesce(excluded.transaction_order, 0) = coalesce(employee_current.transaction_order, 0)
                      AND (excluded.partition_id, excluded.kafka_offset) >
                          (employee_current.partition_id, employee_current.kafka_offset))"""
        )
        connection.execute("COMMIT")

        rejected = connection.execute(
            "SELECT count(*) FROM cdc_new WHERE error_reason IS NOT NULL"
        ).fetchone()[0]
        applied = connection.execute(
                        """SELECT count(*) FROM employee_batch AS batch
                             INNER JOIN employee_current AS current
                                 ON current.employee_id = batch.employee_id
                                AND current.last_event_id = batch.event_id"""
        ).fetchone()[0]
        escaped_path = str(temporary_silver_path).replace("'", "''")
        connection.execute(
            f"COPY (SELECT * FROM employee_current) TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(temporary_silver_path, silver_path)
        return {
            "input": int(input_count),
            "deduplicated": int(deduplicated),
            "applied": int(applied),
            "rejected": int(rejected),
        }
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except duckdb.Error:
            pass
        temporary_silver_path.unlink(missing_ok=True)
        raise
    finally:
        connection.close()


def main() -> None:
    """Spark-submit entry point."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    start_consumer()


if __name__ == "__main__":
    main()
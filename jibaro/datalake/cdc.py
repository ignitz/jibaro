from jibaro.datalake.delta_handler import compact_files
from jibaro.datalake.path import mount_checkpoint_path, mount_path, mount_history_path
from jibaro.settings import settings
from jibaro.utils import path_exists, delete_path
from packaging import version
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.window import Window
from delta import DeltaTable


__all__ = ["kafka_to_raw", "raw_to_staged", "staged_to_curated"]


def kafka_to_raw(
    spark: SparkSession,
    topic: str,
    target_layer: str,
    project_name: str,
    database: str,
    table_name: str,
    options: dict = {},
):
    # [BEGIN] Create DataStreamReader
    # Default values
    df = (
        spark.readStream.format("kafka")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100000)
    )
    bootstrap_servers = settings.kafka_settings["bootstrap_servers"]
    df = df.option("kafka.bootstrap.servers", bootstrap_servers).option(
        "subscribe", topic
    )
    # set custom options
    for k, v in options:
        df = df.option(k, v)
    df = df.load()
    # [END] Create DataStreamReader

    df_write = (
        df.writeStream.trigger(availableNow=True)
        if version.parse(spark.version) >= version.parse("3.3.0")
        else df.writeStream.trigger(once=True)
    )
    df_write.format("delta").start(
        layer=target_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    ).awaitTermination()


def raw_to_staged(
    spark: SparkSession,
    project_name: str,
    database: str,
    table_name: str,
    content_type="avro",
):
    schema_registry_url = settings.schema_registry_url

    if content_type == "avro":
        from jibaro.datalake.avro_handler import avro_handler

        avro_handler(
            spark=spark,
            source_layer="raw",
            target_layer="staged",
            project_name=project_name,
            database=database,
            table_name=table_name,
            schema_registry_url=schema_registry_url,
        )
    elif content_type == "protobuf":
        from jibaro.datalake.protobuf_handler import protobuf_handler

        protobuf_handler(
            spark=spark,
            source_layer="raw",
            target_layer="staged",
            project_name=project_name,
            database=database,
            table_name=table_name,
            schema_registry_url=schema_registry_url,
        )
    else:
        raise NotImplemented


def staged_to_curated(
    spark: SparkSession, project_name: str, database: str, table_name
):
    sc = spark.sparkContext

    source_layer = "staged"
    target_layer = "curated"
    output_path = mount_path(
        layer=target_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )
    checkpoint_location = mount_checkpoint_path(
        layer=target_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )
    history_path = mount_history_path(
        layer=target_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )

    if not path_exists(spark, output_path):
        # delete checkpoint and process entire staged and create folder
        delete_path(spark, checkpoint_location)

    df = spark.readStream.format("delta").load(
        layer=source_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )

    # Option to process avro binary
    def process_delta(df_batch, batch_id):
        print(f"batch_id: {batch_id}")

        # Drop duplication with window partition
        df_batch = (
            df_batch.withColumn(
                "_row_num",
                fn.row_number().over(
                    Window.partitionBy("key").orderBy(fn.col("timestamp").desc())
                ),
            )
            .filter(fn.col("_row_num") == 1)
            .drop("_row_num")
        )

        key_schema_column = (
            "keySchemaId" if "keySchemaId" in df.columns else "keySchema"
        )
        value_schema_column = (
            "valueSchemaId" if "valueSchemaId" in df.columns else "valueSchema"
        )

        distinctSchemaIdDF = (
            df_batch.select(
                key_schema_column,
                value_schema_column,
            )
            .distinct()
            .orderBy(key_schema_column, value_schema_column)
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchemaId = sc.broadcast(valueRow.asDict()[key_schema_column])

            currentValueSchemaId = sc.broadcast(valueRow.asDict()[value_schema_column])

            filterDF = df_batch.filter(
                (fn.col(key_schema_column) == currentKeySchemaId.value)
                & (fn.col(value_schema_column) == currentValueSchemaId.value)
            )

            if not path_exists(spark, output_path):
                dfUpdated = filterDF.filter("value.op != 'd'").select(
                    "value.after.*", "value.op"
                )
                (
                    dfUpdated.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .save(output_path)
                )
            else:
                dt = DeltaTable.forPath(spark, output_path)

                # Need to fill payload before with schema.
                dfUpdated = (
                    filterDF.filter("value.op != 'd'")
                    .select("value.after.*", "value.op")
                    .union(
                        filterDF.filter("value.op = 'd'").select(
                            "value.before.*", "value.op"
                        )
                    )
                )

                (
                    dt.alias("table")
                    .merge(
                        dfUpdated.alias("update"),
                        " AND ".join(
                            [
                                f"table.`{pk}` = update.`{pk}`"
                                for pk in filterDF.select("key.*").columns
                            ]
                        ),
                    )
                    .whenMatchedUpdateAll(condition="update.op != 'd'")
                    .whenNotMatchedInsertAll(condition="update.op != 'd'")
                    .whenMatchedDelete(condition="update.op = 'd'")
                    .execute()
                )
                # end else

            need_compact, numFiles, numOfPartitions = compact_files(
                spark=spark, target_path=output_path
            )

            # Generate metrics
            dt = DeltaTable.forPath(spark, output_path)

            if need_compact:
                max_version = (
                    dt.history(2)
                    .select(fn.max("version").alias("max_version"))
                    .first()
                    .max_version
                )
                dt.history(2).withColumn(
                    "numFiles",
                    fn.when(
                        fn.col("version") == max_version, numOfPartitions
                    ).otherwise(numFiles),
                ).write.format("delta").mode("append").option(
                    "mergeSchema", "true"
                ).save(
                    history_path
                )
            else:
                dt.history(1).withColumn("numFiles", fn.lit(numFiles)).write.format(
                    "delta"
                ).mode("append").option("mergeSchema", "true").save(history_path)

    ###############################################################
    (
        df.writeStream.trigger(once=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(process_delta)
        .start()
        .awaitTermination()
    )
    print("writeStream Done")

    # Generate manifest
    # TODO: Incremental symlink
    dt = DeltaTable.forPath(spark, output_path)
    dt.generate("symlink_format_manifest")

    # Do vacuum process every 25 versions
    version = (
        dt.history(2).select(fn.max("version").alias("max_version")).first().max_version
    )
    if version % 25 == 0 and version > 0:
        dt.vacuum(retentionHours=768)

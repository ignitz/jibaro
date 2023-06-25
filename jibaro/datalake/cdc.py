from jibaro.datalake.delta_handler import compact_files
from jibaro.datalake.path import mount_checkpoint_path, mount_path, mount_history_path
from jibaro.settings import settings
from jibaro.utils import path_exists, delete_path
from packaging import version
import pyspark.sql.functions as fn
import pyspark.sql.types as types
from pyspark.sql.window import Window
from confluent_kafka.schema_registry import SchemaRegistryClient
from delta import DeltaTable


__all__ = ["kafka_to_raw", "raw_to_staged", "staged_to_curated"]


def kafka_to_raw(
    spark, topic, target_layer, project_name, database, table_name, options={}
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


def get_schema_registry_client(schema_registry_url: str):
    schema_registry_conf = {
        "url": schema_registry_url,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    return SchemaRegistryClient(schema_registry_conf)


def avro_handler(
    spark,
    source_layer,
    target_layer,
    project_name,
    database,
    table_name,
    schema_registry_url,
):
    from pyspark.sql.avro.functions import from_avro

    sc = spark.sparkContext

    schema_registry_client = get_schema_registry_client(schema_registry_url)
    fromAvroOptions = {"mode": "FAILFAST"}

    binary_to_string = fn.udf(
        lambda x: str(int.from_bytes(x, byteorder="big")), types.StringType()
    )

    def getSchema(id):
        return str(schema_registry_client.get_schema(id).schema_str)

    df = spark.readStream.format("delta").load(
        layer=source_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )

    # Option to process avro binary

    def process_confluent_schemaregistry(df_batch, batch_id):
        print(f"batch_id: {batch_id}")

        # drop_duplicates only works if key stay in the same partition
        # df_batch = df_batch.orderBy(fn.col('timestamp').desc()).drop_duplicates(['key'])
        df_change = (
            df_batch.withColumn(
                "keySchemaId", binary_to_string(fn.expr("substring(key, 2, 4)"))
            )
            .withColumn("key", fn.expr("substring(key, 6, length(value)-5)"))
            .withColumn(
                "valueSchemaId", binary_to_string(fn.expr("substring(value, 2, 4)"))
            )
            .withColumn("value", fn.expr("substring(value, 6, length(value)-5)"))
        )
        distinctSchemaIdDF = (
            df_change.select(
                fn.col("keySchemaId").cast("integer"),
                fn.col("valueSchemaId").cast("integer"),
            )
            .distinct()
            .orderBy("keySchemaId", "valueSchemaId")
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchemaId = sc.broadcast(valueRow.keySchemaId)
            currentKeySchema = sc.broadcast(getSchema(currentKeySchemaId.value))

            currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
            currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))

            print(f"currentKeySchemaId: {currentKeySchemaId}")
            print(f"currentKeySchema: {currentKeySchema}")
            print(f"currentValueSchemaId: {currentValueSchemaId}")
            print(f"currentValueSchema: {currentValueSchema}")

            filterDF = df_change.filter(
                (fn.col("keySchemaId") == currentKeySchemaId.value)
                & (fn.col("valueSchemaId") == currentValueSchemaId.value)
            )

            (
                filterDF.select(
                    from_avro("key", currentKeySchema.value, fromAvroOptions).alias(
                        "key"
                    ),
                    from_avro("value", currentValueSchema.value, fromAvroOptions).alias(
                        "value"
                    ),
                    "topic",
                    "partition",
                    "offset",
                    "timestamp",
                    "timestampType",
                    fn.col("keySchemaId").cast("integer"),
                    fn.col("valueSchemaId").cast("integer"),
                )
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(
                    mount_path(
                        layer=target_layer,
                        project_name=project_name,
                        database=database,
                        table_name=table_name,
                    )
                )
            )

    ###############################################################
    (
        df.writeStream.trigger(once=True)
        .option(
            "checkpointLocation",
            mount_checkpoint_path(target_layer, project_name, database, table_name),
        )
        .foreachBatch(process_confluent_schemaregistry)
        .start()
        .awaitTermination()
    )
    print("writeStream Done")


def protobuf_handler(
    spark,
    source_layer,
    target_layer,
    project_name,
    database,
    table_name,
    schema_registry_url,
):
    from pyspark.sql.protobuf.functions import from_protobuf

    sc = spark.sparkContext

    schema_registry_client = get_schema_registry_client(schema_registry_url)

    # TODO: Refactor reuse
    binary_to_string = fn.udf(
        lambda x: str(int.from_bytes(x, byteorder="big")), types.StringType()
    )

    def getSchema(id):
        return str(schema_registry_client.get_schema(id).schema_str)

    df = spark.readStream.format("delta").load(
        layer=source_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )

    # [BEGIN] process_confluent_schemaregistry

    def process_confluent_schemaregistry(df_batch, batch_id):
        print(f"batch_id: {batch_id}")

        # I dunno but I need to jump 7 bytes instead of 6
        df_change = (
            df_batch.withColumn(
                "keySchemaId", binary_to_string(fn.expr("substring(key, 2, 4)"))
            )
            .withColumn("key", fn.expr("substring(key, 7, length(value)-6)"))
            .withColumn(
                "valueSchemaId", binary_to_string(fn.expr("substring(value, 2, 4)"))
            )
            .withColumn("value", fn.expr("substring(value, 7, length(value)-6)"))
        )
        distinctSchemaIdDF = (
            df_change.select(
                fn.col("keySchemaId").cast("integer"),
                fn.col("valueSchemaId").cast("integer"),
            )
            .distinct()
            .orderBy("keySchemaId", "valueSchemaId")
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchemaId = sc.broadcast(valueRow.keySchemaId)
            currentKeySchema = sc.broadcast(getSchema(currentKeySchemaId.value))

            currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
            currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))

            print(f"currentKeySchemaId: {currentKeySchemaId.value}")
            print(f"currentKeySchema: {currentKeySchema.value}")
            print(f"currentValueSchemaId: {currentValueSchemaId.value}")
            print(f"currentValueSchema: {currentValueSchema.value}")

            filterDF = df_change.filter(
                (fn.col("keySchemaId") == currentKeySchemaId.value)
                & (fn.col("valueSchemaId") == currentValueSchemaId.value)
            )

            from jibaro.utils import generate_proto_descriptors

            return_path_key, return_path_value = generate_proto_descriptors(
                filterDF.first().topic, currentKeySchema.value, currentValueSchema.value
            )

            (
                filterDF.select(
                    from_protobuf(
                        "key", "Key", return_path_key, options={"mode": "FAILFAST"}
                    ).alias("key"),
                    from_protobuf(
                        "value",
                        "Envelope",
                        return_path_value,
                        options={"mode": "FAILFAST"},
                    ).alias("value"),
                    "topic",
                    "partition",
                    "offset",
                    "timestamp",
                    "timestampType",
                    fn.col("keySchemaId").cast("integer"),
                    fn.col("valueSchemaId").cast("integer"),
                )
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(
                    mount_path(
                        layer=target_layer,
                        project_name=project_name,
                        database=database,
                        table_name=table_name,
                    )
                )
            )

    ###############################################################
    # [END] process_confluent_schemaregistry
    ###############################################################

    df_write = (
        df.writeStream.trigger(availableNow=True).option("maxFilesPerTrigger", 1000)
        if version.parse(spark.version) > version.parse("3.3.0")
        else df.writeStream.trigger(once=True)
    )
    (
        df_write.option(
            "checkpointLocation",
            mount_checkpoint_path(target_layer, project_name, database, table_name),
        )
        .foreachBatch(process_confluent_schemaregistry)
        .start()
        .awaitTermination()
    )
    print("writeStream Done")


def raw_to_staged(spark, project_name, database, table_name, content_type="avro"):
    schema_registry_url = settings.schema_registry_url

    if content_type == "avro":
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


def staged_to_curated(spark, project_name, database, table_name):
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
    dt = DeltaTable.forPath(spark, output_path)
    dt.generate("symlink_format_manifest")

    # Do vacuum process every 25 versions
    version = (
        dt.history(2).select(fn.max("version").alias("max_version")).first().max_version
    )
    if version % 25 == 0 and version > 0:
        dt.vacuum(retentionHours=768)

from jibaro.datalake.path import mount_checkpoint_path, mount_path
from packaging import version
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark.sql.types as types
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame


__all__ = ["protobuf_handler"]


def get_schema_registry_client(schema_registry_url: str):
    schema_registry_conf = {
        "url": schema_registry_url,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    return SchemaRegistryClient(schema_registry_conf)


def getSchema(schema_registry_client, id):
    return str(schema_registry_client.get_schema(id).schema_str)


binary_to_string = fn.udf(
    lambda x: str(int.from_bytes(x, byteorder="big")), types.StringType()
)


def process_confluent_schemaregistry(
    spark: SparkSession,
    schema_registry_client: SchemaRegistryClient,
    df_batch: DataFrame,
    batch_id: int,
    target_layer: str,
    project_name: str,
    database: str,
    table_name: str,
):
    from pyspark.sql.protobuf.functions import from_protobuf
    from jibaro.utils import generate_proto_descriptors

    sc = spark.sparkContext

    print(f"batch_id: {batch_id}")

    # TODO: Create a conditional to key null
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
        currentKeySchema = sc.broadcast(
            getSchema(schema_registry_client, currentKeySchemaId.value)
        )

        currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
        currentValueSchema = sc.broadcast(
            getSchema(schema_registry_client, currentValueSchemaId.value)
        )

        print(f"currentKeySchemaId: {currentKeySchemaId.value}")
        print(f"currentKeySchema: {currentKeySchema.value}")
        print(f"currentValueSchemaId: {currentValueSchemaId.value}")
        print(f"currentValueSchema: {currentValueSchema.value}")

        filterDF = df_change.filter(
            (fn.col("keySchemaId") == currentKeySchemaId.value)
            & (fn.col("valueSchemaId") == currentValueSchemaId.value)
        )

        topic_name = filterDF.first().topic

        return_path_key, return_path_value = generate_proto_descriptors(
            topic=topic_name,
            value_schema=currentValueSchema.value,
            key_schema=currentKeySchema.value,
        )
        spark.sparkContext.addFile(return_path_key)
        spark.sparkContext.addFile(return_path_value)

        # TODO: Add message_name to the schema Key, Envelope
        (
            filterDF.select(
                from_protobuf(
                    "key",
                    "Key",
                    f"{topic_name}-key.desc",
                    options={"mode": "FAILFAST"},
                ).alias("key"),
                from_protobuf(
                    "value",
                    "Envelope",
                    f"{topic_name}-value.desc",
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


def protobuf_handler(
    spark,
    source_layer,
    target_layer,
    project_name,
    database,
    table_name,
    schema_registry_url,
):
    if version.parse(spark.version) < version.parse("3.4.0"):
        raise Exception("Spark 3.4.0 or higher is required.")

    schema_registry_client = get_schema_registry_client(schema_registry_url)

    df = spark.readStream.format("delta").load(
        layer=source_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )

    df_write = df.writeStream.trigger(availableNow=True).option(
        "maxFilesPerTrigger", 1000
    )
    (
        df_write.option(
            "checkpointLocation",
            mount_checkpoint_path(target_layer, project_name, database, table_name),
        )
        .foreachBatch(
            lambda df_batch, batch_id: process_confluent_schemaregistry(
                spark=spark,
                schema_registry_client=schema_registry_client,
                df_batch=df_batch,
                batch_id=batch_id,
                target_layer=target_layer,
                project_name=project_name,
                database=database,
                table_name=table_name,
            )
        )
        .start()
        .awaitTermination()
    )
    print("writeStream Done")

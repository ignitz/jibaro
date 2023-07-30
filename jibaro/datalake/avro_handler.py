from jibaro.datalake.path import mount_checkpoint_path, mount_path
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame
from jibaro.utils import get_schema_registry_client, getSchema, binary_to_string


__all__ = ["avro_handler"]


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
    from pyspark.sql.avro.functions import from_avro

    sc = spark.sparkContext
    fromAvroOptions = {"mode": "FAILFAST"}

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
        currentKeySchema = sc.broadcast(
            getSchema(schema_registry_client, currentKeySchemaId.value)
        )

        currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
        currentValueSchema = sc.broadcast(
            getSchema(schema_registry_client, currentValueSchemaId.value)
        )

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
                from_avro("key", currentKeySchema.value, fromAvroOptions).alias("key"),
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


def avro_handler(
    spark,
    source_layer,
    target_layer,
    project_name,
    database,
    table_name,
    schema_registry_url,
):
    schema_registry_client = get_schema_registry_client(schema_registry_url)

    df = spark.readStream.format("delta").load(
        layer=source_layer,
        project_name=project_name,
        database=database,
        table_name=table_name,
    )

    (
        df.writeStream.trigger(once=True)
        .option(
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

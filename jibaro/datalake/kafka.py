from jibaro.utils import (
    extract_avro_schema,
    register_schema_registry,
)
from pyspark.sql import functions as fn
from pyspark.sql.avro.functions import to_avro


def dataframe_to_kafka(
    spark,
    df_source,
    topic,
    key_columns=[],
    options={},
):
    column_names = [c for c in df_source.columns if c not in key_columns]

    df_write = (
        df_source.select(
            fn.struct(*key_columns).alias("key"),
            fn.struct(*column_names).alias("value"),
        )
        if len(key_columns) > 0
        else df_source.select(
            fn.lit(None).alias("key"),
            fn.struct(*column_names).alias("value"),
        )
    )

    # Extract Key
    key_avro_schema = (
        extract_avro_schema(
            spark,
            df_write.select("key.*"),
            False,
            "Key",
            topic,
        )
        if len(key_columns) > 0
        else None
    )
    key_id = (
        register_schema_registry(topic + "-key", key_avro_schema)
        if key_avro_schema is not None
        else None
    )

    # Extract Value
    value_avro_schema = extract_avro_schema(
        spark,
        df_write.select("value.*"),
        False,
        "Envelope",
        topic,
    )
    value_id = register_schema_registry(topic + "-value", value_avro_schema)

    columns = []
    if key_id is not None:
        columns.append(
            fn.concat(
                fn.lit(int.to_bytes(0, byteorder="big", length=1)),
                fn.lit(int.to_bytes(key_id, byteorder="big", length=4)),
                to_avro("key"),
            ).alias("key")
        )
    columns.append(
        fn.concat(
            fn.lit(int.to_bytes(0, byteorder="big", length=1)),
            fn.lit(int.to_bytes(value_id, byteorder="big", length=4)),
            to_avro("value"),
        ).alias("value")
    )

    # Write to Kafka topic
    (
        df_write.select(*columns)
        .write.format("kafka")
        .options(**options)
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", topic)
        .save()
    )

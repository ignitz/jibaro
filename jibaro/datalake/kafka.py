from jibaro.utils import (
    register_schema_registry,
    extract_schema,
    generate_proto_descriptors,
)
from pyspark.sql import functions as fn


def convert_column_content(col_name, message_name, desc_path, col_type):
    if col_type == "AVRO":
        from pyspark.sql.avro.functions import to_avro

        return to_avro(col_name)
    elif col_type == "PROTOBUF":
        from pyspark.sql.protobuf.functions import to_protobuf

        return to_protobuf(col_name, message_name, desc_path)
    else:
        raise ValueError("Invalid type")


def dataframe_to_kafka(
    spark,
    df_source,
    topic,
    type="AVRO",
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
    key_schema = (
        extract_schema(
            spark=spark,
            type_format=type,
            catalyst_type=df_write.select("key.*"),
            nullable=False,
            record_name="Key",
            name_space=topic,
        )
        if len(key_columns) > 0
        else None
    )
    key_id = (
        register_schema_registry(
            subject_name=topic + "-key", schema=key_schema, schema_type=type
        )
        if key_schema is not None
        else None
    )

    # Extract Value
    value_schema = extract_schema(
        spark=spark,
        type_format=type,
        catalyst_type=df_write.select("value.*"),
        nullable=False,
        record_name="Envelope",
        name_space=topic,
    )
    value_id = register_schema_registry(
        subject_name=topic + "-value", schema=value_schema, schema_type=type
    )

    key_desc_path, value_desc_path = (
        generate_proto_descriptors(
            topic=topic, key_schema=key_schema, value_schema=value_schema
        )
        if type == "PROTOBUF"
        else (None, None)
    )

    columns = []
    if key_id is not None:
        key_row_content = [
            fn.lit(int.to_bytes(0, byteorder="big", length=1)),
            fn.lit(int.to_bytes(key_id, byteorder="big", length=4)),
        ]
        if type == "PROTOBUF":
            key_row_content.append(fn.lit(int.to_bytes(0, byteorder="big", length=1)))
        key_row_content.append(
            convert_column_content("key", "Key", key_desc_path, type)
        )

        columns.append(fn.concat(*key_row_content).alias("key"))

    value_row_content = [
        fn.lit(int.to_bytes(0, byteorder="big", length=1)),
        fn.lit(int.to_bytes(value_id, byteorder="big", length=4)),
    ]
    if type == "PROTOBUF":
        value_row_content.append(fn.lit(int.to_bytes(0, byteorder="big", length=1)))
    value_row_content.append(
        convert_column_content("value", "Envelope", value_desc_path, type)
    )
    columns.append(fn.concat(*value_row_content).alias("value"))

    # Write to Kafka topic
    (
        df_write.select(*columns)
        .write.format("kafka")
        .options(**options)
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", topic)
        .save()
    )

import json
import requests
from typing import Optional
from jibaro.settings import settings
from pyspark.sql import types as tp
import pyspark.sql.functions as fn
import pyspark.sql.types as types
from confluent_kafka.schema_registry import SchemaRegistryClient


def path_exists(spark, path):
    # spark is a SparkSession
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    ###############################
    # This method support wildcard
    # hpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    # fs = hpath.getFileSystem(sc._jsc.hadoopConfiguration())
    # return len(fs.globStatus(hpath)) > 0


def delete_path(spark, path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        sc._jsc.hadoopConfiguration(),
    )
    if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
        return fs.delete(sc._jvm.org.apache.hadoop.fs.Path(str(path)))
    return False


def extract_avro_schema(spark, catalyst_type, nullable, record_name, name_space):
    return spark._jvm.org.apache.spark.sql.avro.SchemaConverters.toAvroType(
        catalyst_type._jdf.schema(), nullable, record_name, name_space
    )


def register_schema_registry(subject_name, schema, schema_type="AVRO"):
    payload = {"schemaType": schema_type, "schema": str(schema)}
    response = requests.post(
        f"{settings.schema_registry_url}/subjects/{subject_name}/versions",
        data=json.dumps(payload),
        headers={
            "Accept": "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json",
        },
    )
    if response.status_code > 300:
        raise Exception(response.text)
    return response.json()["id"]


def convert_schema_to_proto(schema):
    index = 1
    fields = []
    for field in schema.fields:
        field_type = "string"
        if isinstance(field.dataType, tp.StringType):
            field_type = "string"
        elif isinstance(field.dataType, tp.ShortType):
            field_type = "int32"
        elif isinstance(field.dataType, tp.IntegerType):
            field_type = "int32"
        elif isinstance(field.dataType, tp.LongType):
            field_type = "int64"
        elif isinstance(field.dataType, tp.FloatType):
            field_type = "float"
        elif isinstance(field.dataType, tp.DoubleType):
            field_type = "double"
        elif isinstance(field.dataType, tp.BooleanType):
            field_type = "bool"
        elif isinstance(field.dataType, tp.TimestampType):
            field_type = "int64"
        elif isinstance(field.dataType, tp.BinaryType):
            field_type = "bytes"
        elif isinstance(field.dataType, tp.DateType):
            field_type = "int32"
        elif isinstance(field.dataType, tp.TimestampType):
            field_type = "int64"
        else:
            raise Exception(f"Unsupported data type: {field.dataType}")
        fields.append(f"  {field_type} {field.name} = {index};")
        index += 1
    return fields


def generate_proto(spark, catalyst_type, message_name="Message", topic="topic"):
    proto_message = (
        """syntax = "proto3";\n\n"""
        + f"""package {topic};\n\n"""
        + """message """
        + message_name
        + """ {\n// Define your fields here\n}"""
    )
    fields = convert_schema_to_proto(schema=catalyst_type.schema)
    return proto_message.replace("// Define your fields here", "\n".join(fields))


def extract_schema(
    spark, type_format, catalyst_type, nullable, record_name, name_space
):
    if type_format == "AVRO":
        return extract_avro_schema(
            spark,
            catalyst_type,
            nullable,
            record_name,
            name_space,
        )
    elif type_format == "PROTOBUF":
        return generate_proto(
            spark,
            catalyst_type,
            record_name,
            name_space,
        )
    else:
        raise Exception(f"Unsupported format: {type_format}")


def generate_proto_descriptors(
    topic: str, value_schema: str, key_schema: Optional[str] = None
) -> tuple[str, str]:
    import grpc_tools.protoc as protoc
    import os

    TEMP_FOLDER = f"/tmp/pipeline/protobuf/{topic}"
    # TODO: need to know the spark work dir of Amazon EMR
    # Spark on Kubernetes default work dir

    SPARK_DIR = "/opt/spark/work-dir"
    SPARK_DIR = os.getcwd()

    os.makedirs(TEMP_FOLDER, exist_ok=True)

    if key_schema is not None:
        with open(TEMP_FOLDER + f"/key.proto", "w") as w:
            w.write(key_schema)
        protoc.main(
            [
                "--include_imports",
                f"--proto_path={TEMP_FOLDER}/",
                f"--descriptor_set_out={SPARK_DIR}/{topic}-key.desc",
                f"key.proto",
            ]
        )

    with open(TEMP_FOLDER + f"/value.proto", "w") as w:
        w.write(value_schema)
    protoc.main(
        [
            "--include_imports",
            f"--proto_path={TEMP_FOLDER}/",
            f"--descriptor_set_out={SPARK_DIR}/{topic}-value.desc",
            "value.proto",
        ]
    )

    return_path_key = (
        f"{SPARK_DIR}/{topic}-key.desc" if key_schema is not None else None
    )
    return_path_value = f"{SPARK_DIR}/{topic}-value.desc"
    return return_path_key, return_path_value


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

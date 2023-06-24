import json
import requests
from jibaro.settings import settings


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

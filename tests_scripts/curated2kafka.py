from jibaro.spark.session import JibaroSession
from jibaro.datalake.kafka import dataframe_to_kafka
import sys

topic = sys.argv[1]
project_name = sys.argv[2]
database = sys.argv[3]
table_name = sys.argv[4]

spark = JibaroSession.builder.appName("Spark Kafka writer").getOrCreate()

df = spark.read.format("delta").load(
    layer="curated",
    project_name=project_name,
    database=database,
    table_name=table_name,
)

dataframe_to_kafka(
    spark=spark,
    df_source=df,
    topic=topic,
    key_columns=["id"],
)

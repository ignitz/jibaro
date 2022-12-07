from jibaro.spark.session import JibaroSession
from jibaro.datalake.cdc import kafka_to_raw
import sys

topic = sys.argv[1]
project_name = sys.argv[2]
database = sys.argv[3]
table_name = sys.argv[4]

spark = JibaroSession.builder.appName("Spark Streaming Delta").getOrCreate()


kafka_to_raw(
    spark=spark,
    topic=topic,
    target_layer='raw',
    project_name=project_name,
    database=database,
    table_name=table_name,
)

# For Spark 3.1.x
spark.stop()

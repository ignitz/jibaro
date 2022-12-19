# Jibaro

Library to use and organize data pipelines on Data Lakes.

## Installation

**NOT YET PUBLISHED**

```bash
pip install jibaro
```

## Usage

Read DataFrame from a table in a Data Lake.

```python
from jibaro.spark.session import JibaroSession

spark = JibaroSession.builder.appName("Jibaro Session").getOrCreate()

layer = "raw"
project_name = "my_project"
database = "my_database"
table_name = "my_table"

df = spark.read.parquet(
    layer=source_layer, project_name=project_name, database=database, table_name=table_name
)

df.show()
```

Read DataStream from a table in a Data Lake.

```python
from jibaro.spark.session import JibaroSession

spark = JibaroSession.builder.appName("Jibaro Session").getOrCreate()

layer = "raw"
project_name = "my_project"
database = "my_database"
table_name = "my_table"

df_stream = spark.readStream.parquet(
    layer=source_layer, project_name=project_name, database=database, table_name=table_name
)

(
        df
        .writeStream
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda batch_df, batch_id: batch_df.show())
        .start().awaitTermination()
)
```

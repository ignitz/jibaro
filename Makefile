.PHONY: help
help: ## Show help menu
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build:
	@poetry build

testavro: ## Test Avro pipeline
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/kafka2raw.py 'dbserver1.inventory.products' 'dbserver1' 'inventory' 'products'
	@spark-submit --packages org.apache.spark:spark-avro_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/raw2staged.py 'dbserver1' 'inventory' 'products' avro
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/staged2curated.py 'dbserver1' 'inventory' 'products'

testprotobuf: ## Test Protobuf pipeline
	@spark-submit --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/kafka2raw.py 'protobuf.inventory.products' 'protobuf' 'inventory' 'products'
	@spark-submit --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/raw2staged.py 'protobuf' 'inventory' 'products' protobuf
	@spark-submit --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/staged2curated.py 'protobuf' 'inventory' 'products'

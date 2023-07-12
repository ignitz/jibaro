.PHONY: help
help: ## Show help menu
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build:
	@poetry build

.PHONY: lake-setup
lake-setup: ## 🌊 Setup Confluent lake with docker compose, create buckets and Debezium's connectors
	$(MAKE) -C lake_lab lake-setup

.PHONY: lake-destroy
lake-destroy: ## 🗑 Destroy Confluent lake with docker compose
	$(MAKE) -C lake_lab lake-destroy

.PHONY: client-setup
client-setup: ## 🐰 Setup Trino, Hive
	$(MAKE) -C lake_lab client-setup

.PHONY: client-destroy
client-destroy: ## 🥕 Destroy Trino, Hive
	$(MAKE) -C lake_lab client-destroy

testavro: ## Test Avro pipeline
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/kafka2raw.py 'dbserver1.inventory.products' 'dbserver1' 'inventory' 'products'
	@spark-submit --packages org.apache.spark:spark-avro_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/raw2staged.py 'dbserver1' 'inventory' 'products' avro
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/staged2curated.py 'dbserver1' 'inventory' 'products'

testprotobuf: ## Test Protobuf pipeline
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/kafka2raw.py 'protobuf.inventory.products' 'protobuf' 'inventory' 'products'
	@spark-submit --packages org.apache.spark:spark-protobuf_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/raw2staged.py 'protobuf' 'inventory' 'products' protobuf
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/staged2curated.py 'protobuf' 'inventory' 'products'

testwriteavrokafka: ## Test write to topic
	@curl -X DELETE http://localhost:8081/subjects/output_avro.inventory.products-key
	@curl -X DELETE http://localhost:8081/subjects/output_avro.inventory.products-key/\?permanent\=true
	@curl -X DELETE http://localhost:8081/subjects/output_avro.inventory.products-value
	@curl -X DELETE http://localhost:8081/subjects/output_avro.inventory.products-value/\?permanent\=true
	@docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --topic output_avro.inventory.products --delete --if-exists
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-avro_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/curated2kafka_avro.py 'output_avro.inventory.products' 'dbserver1' 'inventory' 'products'

testwriteprotokafka: ## Test write to topic
	@curl -X DELETE http://localhost:8081/subjects/output_proto.inventory.products-key
	@curl -X DELETE http://localhost:8081/subjects/output_proto.inventory.products-key/\?permanent\=true
	@curl -X DELETE http://localhost:8081/subjects/output_proto.inventory.products-value
	@curl -X DELETE http://localhost:8081/subjects/output_proto.inventory.products-value/\?permanent\=true
	@docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --topic output_proto.inventory.products --delete --if-exists
	@spark-submit --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-protobuf_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/curated2kafka_proto.py 'output_proto.inventory.products' 'dbserver1' 'inventory' 'products'

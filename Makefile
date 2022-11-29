.PHONY: help
help: ## Show help menu
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

raw: ## raw
	@spark-submit tests_scripts/kafka2raw.py 'dbserver1.inventory.products'

staged: ## staged
	@spark-submit tests_scripts/raw2staged.py 'dbserver1' 'inventory' 'products' avro

curated: ## curated
	@spark-submit tests_scripts/staged2curated.py 'dbserver1' 'inventory' 'products'

test: ## test
	spark-submit --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/kafka2raw.py 'protobuf.inventory.products'
	spark-submit --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/raw2staged.py 'protobuf' 'inventory' 'products' protobuf
	spark-submit --executor-memory 3g --driver-memory 6g --properties-file "$(PWD)/tests_scripts/spark.properties" tests_scripts/staged2curated.py 'protobuf' 'inventory' 'products'

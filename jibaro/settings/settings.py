__all__ = ['settings']

import toml
import os
from pydantic import BaseSettings


config = toml.load(os.path.join(os.path.dirname(__file__), 'default.toml'))


class Settings(BaseSettings):
    # Storage configs
    prefix_protocol: str = config['bucket']['storage_system']

    raw: str = config['bucket']['raw']
    staged: str = config['bucket']['staged']
    curated: str = config['bucket']['curated']
    spark_control: str = config['bucket']['spark_control']

    # Kafka configurations
    kafka_settings = {
        'bootstrap_servers': config['kafka']['bootstrap_servers'],
        'tls': config['kafka']['tls_enable']
    }
    schema_registry_url: str = config['kafka']['schema_registry']

    # Delta Lake confis
    max_num_files_allowed: int = config['deltalake']['max_num_files_allowed']


settings = Settings()

from autoMatch.constants import *
from autoMatch.utils.common import read_yaml, create_directories
from autoMatch import logger

from autoMatch.entity.config_entity import DataIngestionConfig

class ConfigurationManager:
    def __init__(
        self,
        config_filepath = CONFIG_FILE_PATH,
        params_filepath = PARAMS_FILE_PATH,
        schema_filepath = SCHEMA_FILE_PATH):

        self.config = read_yaml(config_filepath)
        self.params = read_yaml(params_filepath)
        self.schema = read_yaml(schema_filepath)

        create_directories([self.config.artifacts_root])

    
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion
        schema = self.schema.data_ingestion

        create_directories([config.root_dir])

        data_ingestion_config = DataIngestionConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            output_table = config.output_table,
            columns = schema.columns,
            start_date = schema.date_range.start_date,
            end_date = schema.date_range.end_date
        )

        return data_ingestion_config
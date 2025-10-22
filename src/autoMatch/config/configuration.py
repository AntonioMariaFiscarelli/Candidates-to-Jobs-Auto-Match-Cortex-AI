from autoMatch.constants import *
from autoMatch.utils.common import read_yaml, create_directories
from autoMatch import logger

from autoMatch.entity.config_entity import DataIngestionConfig, DataValidationConfig, DataTransformationConfig, SearchEngineConfig, LLMConfig

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
            italian_cities_file = config.italian_cities_file,
            output_table_italian_cities = config.output_table_italian_cities,
            columns = schema.columns,
            start_date = schema.date_range.start_date,
            end_date = schema.date_range.end_date,
            italian_cities_string_columns = schema.cities_file_columns.string_columns,
            italian_cities_numeric_columns = schema.cities_file_columns.numeric_columns,
        )

        return data_ingestion_config
    

    def get_data_validation_config(self) -> DataValidationConfig:
        config = self.config.data_validation
        schema = self.schema.data_validation

        create_directories([config.root_dir])

        data_validation_config = DataValidationConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            table_schema=schema.table_schema,
            status_file = config.STATUS_FILE
        )

        return data_validation_config
    

    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation

        create_directories([config.root_dir])

        data_ingestion_config = DataTransformationConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            input_table_cleaned=config.input_table_cleaned,
            input_table_italian_cities = config.input_table_italian_cities,
            output_table = config.output_table,
        )

        return data_ingestion_config
    

    def get_search_engine_config(self) -> SearchEngineConfig:
        config = self.config.search_engine
        schema = self.schema.search_engine

        create_directories([config.root_dir])

        search_engine_config = SearchEngineConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            search_service=config.search_service,
            search_columns=schema.search_columns,
            attributes_columns = schema.attributes_columns,
            columns = schema.columns
        )

        return search_engine_config
    

    def get_llm_config(self) -> LLMConfig:
        config = self.config.llm
        schema = self.schema.llm
        params = self.params.llm

        create_directories([config.root_dir])

        llm_config = LLMConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            llm_name=params.llm_name,
            columns=schema.columns,

        )

        return llm_config
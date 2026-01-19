from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: str
    database: str
    schema: str
    input_table: str
    output_table: str
    italian_cities_file: str
    output_table_italian_cities: str
    columns: dict
    start_date: str
    end_date: str
    days_prior: int
    desired_locations: dict
    parttime_preferenza_perc: list[int]
    italian_cities_string_columns: dict
    italian_cities_numeric_columns: dict

@dataclass(frozen=True)
class DataValidationConfig:
    root_dir: str
    database: str
    schema: str
    input_table: dict
    table_schema: dict
    status_file: str

@dataclass(frozen=True)
class DataTransformationConfig:
    root_dir: str
    database: str
    schema: str
    input_table: str
    input_table_cleaned: str
    input_table_italian_cities: str
    output_table: str
    education_levels: dict

@dataclass(frozen=True)
class SearchEngineConfig:
    root_dir: str
    database: str
    schema: str
    input_table: str
    search_columns : dict
    attributes_columns: dict
    columns: dict
    search_service: str

@dataclass(frozen=True)
class LLMConfig:
    root_dir: str
    database: str
    schema: str
    input_table: str
    columns : dict
    llm_name : str
    columns: dict
    role_mappings: dict
    languages: list[str]
    education_levels : dict
    desired_locations: dict
    turno_preferenza: dict
    parttime_preferenza_perc: list[int]
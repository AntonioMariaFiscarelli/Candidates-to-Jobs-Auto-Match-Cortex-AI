from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: str
    database: str
    schema: str
    input_table: str
    output_table: str
    columns: dict
    start_date: str
    end_date: str

@dataclass(frozen=True)
class DataValidationConfig:
    root_dir: str
    database: str
    schema: str
    input_table: str
    columns: dict
    status_file: str
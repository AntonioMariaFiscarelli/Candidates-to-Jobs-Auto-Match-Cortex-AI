"""Paths of the project."""
import os
from pathlib import Path
#from dotenv import load_dotenv

#ROOT_PATH = Path(__file__).parent.parent
ROOT_PATH = Path(__file__).parent.parent.parent.parent

ENV_PATH = ROOT_PATH / ".env"
SNOWFLAKE_SESSION_PARAMETERS_PATH = ROOT_PATH / "snowflake-session-parameters.json"

"""
CONFIG_FILE_PATH = Path("config/config.yaml")
PARAMS_FILE_PATH = Path("params.yaml")
SCHEMA_FILE_PATH = Path("schema.yaml")
"""

#load_dotenv()
#env = os.getenv("ENV", "test") # default to test

env = "test"
env = "dev"

CONFIG_FILE_PATH = ROOT_PATH / "config" / f"config_{env}.yaml"
PARAMS_FILE_PATH = ROOT_PATH / "params.yaml"
SCHEMA_FILE_PATH = ROOT_PATH / "schema.yaml"

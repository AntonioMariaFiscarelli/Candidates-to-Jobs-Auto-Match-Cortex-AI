import os
import yaml
from src.autoMatch import logger
import json
#from ensure import ensure_annotations
#from box import ConfigBox
#from box.exceptions import BoxValueError
from src.autoMatch.utils.box.config_box import ConfigBox
from src.autoMatch.utils.box.exceptions import BoxValueError
from pathlib import Path
from typing import Any

from snowflake.snowpark import functions as F

from snowflake.snowpark.functions import col, trim, lower, when, lit, trim

import math

#@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """reads yaml file and returns

    Args:
        path_to_yaml (str): path like input

    Raises:
        ValueError: if yaml file is empty
        e: empty file

    Returns:
        ConfigBox: ConfigBox type
    """
    try:
        with open(path_to_yaml) as yaml_file:
            content = yaml.safe_load(yaml_file)
            logger.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError:
        raise ValueError("yaml file is empty")
    except Exception as e:
        raise e
    


#@ensure_annotations
def create_directories(path_to_directories: list, verbose=True):
    """create list of directories

    Args:
        path_to_directories (list): list of path of directories
        ignore_log (bool, optional): ignore if multiple dirs is to be created. Defaults to False.
    """
    for path in path_to_directories:
        os.makedirs(path, exist_ok=True)
        if verbose:
            logger.info(f"created directory at: {path}")


#@ensure_annotations
def save_json(path: Path, data: dict):
    """save json data

    Args:
        path (Path): path to json file
        data (dict): data to be saved in json file
    """
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

    logger.info(f"json file saved at: {path}")




#@ensure_annotations
def load_json(path: Path) -> ConfigBox:
    """load json files data

    Args:
        path (Path): path to json file

    Returns:
        ConfigBox: data as class attributes instead of dict
    """
    with open(path) as f:
        content = json.load(f)

    logger.info(f"json file loaded succesfully from: {path}")
    return ConfigBox(content)


#@ensure_annotations
def save_bin(data: Any, path: Path):
    """save binary file

    Args:
        data (Any): data to be saved as binary
        path (Path): path to binary file
    """
    joblib.dump(value=data, filename=path)
    logger.info(f"binary file saved at: {path}")


#@ensure_annotations
def load_bin(path: Path) -> Any:
    """load binary data

    Args:
        path (Path): path to binary file

    Returns:
        Any: object stored in the file
    """
    data = joblib.load(path)
    logger.info(f"binary file loaded from: {path}")
    return data



#@ensure_annotations
def get_size(path: Path) -> str:
    """get size in KB

    Args:
        path (Path): path of the file

    Returns:
        str: size in KB
    """
    size_in_kb = round(os.path.getsize(path)/1024)
    return f"~ {size_in_kb} KB"

#@ensure_annotations
def validate_string(df, column_name):
    df = df.with_column(
        column_name,
        when(
            (col(column_name).is_not_null()) &
            (trim(col(column_name)) != "") &
            (~lower(trim(col(column_name))).isin(["null", "none", "nan"])),
            col(column_name)
            ).otherwise(lit(None))
        )
    return df

#@ensure_annotations
def is_valid_number(x):
    return isinstance(x, (int, float)) and not math.isnan(x)

def haversine(lat_ref, lon_ref):
    # --- Haversine distance calculation in Snowpark ---
    lat1 = F.radians(F.lit(lat_ref))
    lon1 = F.radians(F.lit(lon_ref))
    lat2 = F.radians(F.col("LATITUDE"))
    lon2 = F.radians(F.col("LONGITUDE"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = F.sin(dlat / 2) * F.sin(dlat / 2) + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) * F.sin(dlon / 2)
    c = 2 * F.asin(F.sqrt(a))
    distance_km = 6371 * c  # Earth radius in km

    return distance_km
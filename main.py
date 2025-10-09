from autoMatch import logger
from autoMatch.utils.snowflake_utils import get_snowpark_session

from autoMatch.pipeline.stage_01_data_ingestion import DataIngestionTrainingPipeline
from autoMatch.pipeline.stage_02_data_validation import DataValidationTrainingPipeline
from autoMatch.pipeline.stage_03_data_transformation import DataTransformationTrainingPipeline

import logging
logging.getLogger("snowflake").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("snowflake.snowpark").setLevel(logging.WARNING)


session = get_snowpark_session()


STAGE_NAME = "Data Ingestion stage"
try:
   logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<") 
   data_ingestion = DataIngestionTrainingPipeline()
   data_ingestion.main(session)
   logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
    logger.exception(e)
    raise e


STAGE_NAME = "Data Validation stage"
try:
   logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<") 
   data_validation = DataValidationTrainingPipeline()
   data_validation.main(session)
   logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
    logger.exception(e)
    raise e

'''
STAGE_NAME = "Data Transformation stage"
try:
   logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
   obj = DataTransformationTrainingPipeline()
   obj.main(session)
   logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
   logger.exception(e)
   raise e
'''
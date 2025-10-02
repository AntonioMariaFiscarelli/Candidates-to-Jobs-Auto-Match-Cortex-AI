from autoMatch import logger
from autoMatch.utils.snowflake_utils import get_snowpark_session

from autoMatch.pipeline.stage_01_data_ingestion import DataIngestionTrainingPipeline

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
from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.data_ingestion import DataIngestion
from autoMatch import logger

from autoMatch.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Data Ingestion stage"

class DataIngestionTrainingPipeline:
    def __init__(self):
        pass

    def main(self, session):
        config = ConfigurationManager()
        data_ingestion_config = config.get_data_ingestion_config()
        data_ingestion = DataIngestion(config=data_ingestion_config)
        df = data_ingestion.read_table(session)
        data_ingestion.write_table(session, df)


    
if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        session = get_snowpark_session()
        obj = DataIngestionTrainingPipeline(session)
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e
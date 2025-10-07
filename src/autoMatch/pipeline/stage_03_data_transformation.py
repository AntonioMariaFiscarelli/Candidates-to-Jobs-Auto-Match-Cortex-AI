from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.data_transformation import DataTransformation
from autoMatch import logger

from autoMatch.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Data Transformnation stage"

class DataTransformationTrainingPipeline:
    def __init__(self):
        pass

    def main(self, session):
        config = ConfigurationManager()
        data_transformation_config = config.get_data_transformation_config()
        data_transformation = DataTransformation(config=data_transformation_config)
        data_transformation.clean_description(session)
        df = data_transformation.apply_ner_cortexai(session)
        data_transformation.write_table(session, df)




if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        session = get_snowpark_session()
        obj = DataTransformationTrainingPipeline(session)
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e
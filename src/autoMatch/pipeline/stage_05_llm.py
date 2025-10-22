from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.llm import LLM
from autoMatch import logger

from autoMatch.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "LLM stage"

class LLMTrainingPipeline:
    def __init__(self):
        pass

    def main(self, session):
        config = ConfigurationManager()
        llm_config = config.get_llm_config()
        llm = LLM(config=llm_config)
        df = session.sql("SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_CANDIDATE_FEATURES LIMIT 100")
        print(df.columns)
        prompt = llm.create_prompt("Data Scientist", "Python, Java", df)
        print(prompt)
        response = llm.call_ai(session, prompt)
        print(response)




if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        session = get_snowpark_session()
        obj = LLMTrainingPipeline(session)
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e
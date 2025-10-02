from autoMatch import logger
from snowflake.snowpark.functions import col

from autoMatch.entity.config_entity import DataIngestionConfig


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def read_table(self, session):
        """
        Reads input table
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table
        columns = self.config.columns
        start_date = self.config.start_date
        end_date = self.config.end_date


        print(start_date)
        print(end_date)
        df = session.table(f"{database}.{schema}.{input_table}")
        df = df.select([col(c) for c in columns])
        df = df.filter((col("date_added") >= start_date) & (col("date_added") <= end_date))
        logger.info(f"Table {input_table} successfully read. Number of rows: {df.count()}")

        return df


    def write_table(self, session, df):
        """
        Writes input table
        Function returns nothing
        """
        output_table = self.config.output_table

        df.write.save_as_table(output_table, mode="overwrite")
        logger.info(f"Table {output_table} successfully written")

  

from autoMatch import logger
import pandas as pd
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

        df = session.table(f"{database}.{schema}.{input_table}")
        df = df.select([col(c) for c in columns])
        df = df.filter((col("date_added") >= start_date) & (col("date_added") <= end_date))
        logger.info(f"Table {input_table} successfully read. Number of rows: {df.count()}")

        return df

    def read_cities_file(self, session):
        """
        Reads XLSX file containing italian cities
        Function returns Snowflake dataframe
        """
        italian_cities_file = self.config.italian_cities_file
        string_columns = self.config.italian_cities_string_columns
        numeric_columns = self.config.italian_cities_numeric_columns

        df = pd.read_excel(italian_cities_file, header=0)

        # Rename columns for consistency (optional but recommended)
        df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
        df = df[string_columns + numeric_columns]
        
        # Convert ZIP to string (preserve leading zeros)
        df["zip"] = df["zip"].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
        
        # Convert string columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()

        # Convert latitude and longitude to float, handle NaNs
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info(f"XLSX file containing italian cities successfully read")
        print(df.head(3))
        print(df.info())

        return session.create_dataframe(df)

    def write_table(self, df, table_name = 'output_table'):
        """
        Writes table
        Function returns nothing
        """

        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")

  


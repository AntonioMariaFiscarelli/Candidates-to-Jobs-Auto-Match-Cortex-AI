from autoMatch import logger
from snowflake.snowpark.functions import col, trim, lower, length, expr
from snowflake.snowpark.types import StringType, BooleanType

from snowflake.snowpark.functions import udf

from autoMatch.entity.config_entity import DataTransformationConfig


class DataTransformation:
    def __init__(self, config: DataTransformationConfig):
        self.config = config


    def clean_description(self, session):
        """
        Reads input table
        Cleans description column:
            - removes rows with empty description
            - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
            - removes all html tags
            - lowercases all text
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table
        input_table_cleaned = self.config.input_table_cleaned

        df = session.table(f"{database}.{schema}.{input_table}")#.limit(100)
        df = df.filter((col("description").is_not_null()) & (trim(col("description")) != ""))

        def build_normalize_whitespace_udf():
            def normalize(text: str) -> str:
                import re
                if text is None:
                    return ''
                return re.sub(r'[ \t]+', ' ', text).strip()

            return udf(normalize, return_type=StringType(), input_types=[StringType()])
        normalize_udf = build_normalize_whitespace_udf()
        df = df.with_column("description", normalize_udf(df["description"]))
        

        def build_clean_html_udf():
            from bs4 import BeautifulSoup
            def clean_html(text: str) -> str:
                if not text:
                    return ""
                return BeautifulSoup(text, "html.parser").get_text()

            return udf(
                clean_html,
                return_type=StringType(),
                input_types=[StringType()],
                packages=["beautifulsoup4"]
            )

        clean_html_udf = build_clean_html_udf()
        df = df.with_column("description", clean_html_udf(df["description"]))

        df = df.with_column("description", lower(df["description"]))

        df = df.filter((col("description").is_not_null()) & 
                       (trim(col("description")) != "") &
                       (length(trim(col("description"))) > 5) &
                       (~col("description").like("%None%")) &
                       (~col("description").like("%null%"))
                       )

        df = df.with_column("description", col("description").cast("STRING"))

        def build_remove_special_chars_udf():
            import re
            def remove_special_chars(text: str) -> str:
                if not text:
                    return ""
                return re.sub(r'[^a-zA-Z0-9\s]', '', text)
            return udf(remove_special_chars,return_type=StringType(),input_types=[StringType()])
        remove_special_chars_udf = build_remove_special_chars_udf()
        df = df.with_column("description", remove_special_chars_udf(df["description"]))


        df.write.save_as_table(
            input_table_cleaned,
            mode="overwrite",
        )
        logger.info(f"Table {input_table} successfully cleaned")

        return df

    def apply_ner_cortexai(self, session):
        """
        Reads input table

        Performs Named Entity Recognition
        Function returns Snowflake dataframe
        """

        from snowflake.snowpark.functions import parse_json

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.input_table_cleaned

        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Estrai dal seguente testo i campi: age (numero), date_of_birth (YYYY-MM-DD), location (stringa), zip_code (numero), last_job, second_last_job, third_last_job, skills (stringa). ',
                        'Rispondi in formato JSON, senza testo extra, attieniti a questo esempio: {{"age": 30, "date_of_birth": "1993-05-12", "location": "Milano", "zip_code": 20100, "last_job": "Data Engineer", "second_last_job": "Developer", "third_last_job": "Intern", "skills": "Python, SQL"}}. ',
                        'Testo: ', description
                    )
                ) AS ner_json
            FROM {database}.{schema}.{input_table_cleaned}

            """

        df = session.sql(query)

        def build_clean_parsing_udf():
            def clean(x: str) -> str:
                if x is None:
                    return ''
                x = x.lower().lstrip()
                if x.startswith("```json"):
                    x = x[8:].lstrip()
                x = x.replace('\n', ' ').replace('\t', ' ').replace('\\', '').strip()
                x = ' '.join(x.split())
                if x.endswith("```"):
                    x = x[:-3].rstrip()
                if x.endswith("'") or x.endswith('"'):
                    x = x[:-1].rstrip()
                return x

            return udf(clean, return_type=StringType(), input_types=[StringType()])
        clean_udf = build_clean_parsing_udf()

        df = df.with_column("ner_json", clean_udf(df["ner_json"]))

        def build_is_valid_json_udf():
            import json
            def is_valid(text: str) -> bool:
                if not text:
                    return False
                try:
                    json.loads(text)
                    return True
                except Exception:
                    return False

            return udf(is_valid, return_type=BooleanType(), input_types=[StringType()])
        is_valid_json_udf = build_is_valid_json_udf()
        df = df.with_column("is_valid_json", is_valid_json_udf(df["ner_json"]))
        df = df.filter(col("is_valid_json") == True)
        df = df.drop("is_valid_json")

        df = df.with_column("ner_json", parse_json(col("ner_json")))

        df = df.filter(df["ner_json"].is_not_null())

        df = df.with_columns(
            ["age", "date_of_birth", "location", "zip_code", "last_job", "second_last_job", "third_last_job", "skills"],
            [
                col("ner_json")["age"].cast("STRING"),
                col("ner_json")["data_of_birth"].cast("STRING"),
                col("ner_json")["location"].cast("STRING"),
                col("ner_json")["zip_code"].cast("STRING"), 
                col("ner_json")["last_job"].cast("STRING"),
                col("ner_json")["second_last_job"].cast("STRING"),
                col("ner_json")["third_last_job"].cast("STRING"),
                col("ner_json")["skills"].cast("STRING")
            ]
        )

        df = df.drop("ner_json")

        logger.info(f"NER on {input_table_cleaned} table successful")


        return df
    

    def write_table(self, session, df):
        """
        Writes input table
        Function returns nothing
        """
        output_table = self.config.output_table

        df.write.save_as_table(output_table, mode="overwrite")
        logger.info(f"Table {output_table} successfully written")

  

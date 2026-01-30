from src.autoMatch import logger
from snowflake.snowpark.functions import col, trim, lower, length, parse_json, when, lit, trim, to_date, to_varchar
from snowflake.snowpark.types import StringType, BooleanType
from snowflake.snowpark.functions import udf
from datetime import date

from src.autoMatch.utils.common import validate_string

from src.autoMatch.entity.config_entity import DataTransformationConfig


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

        df = session.table(f"{database}.{schema}.{input_table}")
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
        
        logger.info(f"Table {input_table} successfully cleaned")

        return df
    
    def apply_ner_cortexai(self, session):
        """
        Reads input table cleaned
        Performs Named Entity Recognition:
            - age
            - date_of_birth
            - location
            - zip_code
            - last_job
            - second_last_job
            - third_last_job
            - skills
        Function returns Snowflake dataframe
        """

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.input_table_cleaned
        education_levels = self.config.education_levels
        regions = self.config.regions

        today_string = date.today().strftime("%Y-%m-%d")

        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Estrai dal seguente testo i campi: 
                        age (stringa. calcolala basandoti su date_of_birth, considerando che la data di oggi e {today_string};
                            esempio: se date_of_birth = 1990-05-18 e oggi siamo in data 2025-09-15, age = 35 
                            se non è possibile calcolarla, restituisci NaN), 
                        date_of_birth (stringa in formato YYYY-MM-DD, 
                            YYYY deve essere compreso tra 1900 e {today_string},
                            MM deve essere compreso tra 1 e 12,
                            DD deve essere compreso tra: 
                                1 e 30 quando MM uguale a 11, 04, 06, 09;
                                1 e 28 quando MM uguale a 02
                                1 e 31 per i restanti casi
                            ), 
                        location (stringa. il domicilio. solo la citta, non includere province o altro), 
                        region (la regione corrispondente alla citta del domicilio. stringa. possibili valori (solo uno): {", ".join(regions)},
                        country (la nazione corrispondente alla citta del domicilio. stringa, solo in Italiano. Esempio: Italia, Spagna, Iran etc.),
                        zip_code (cap. 5 caratteri numerici), 
                        last_job, 
                        second_last_job, 
                        third_last_job, 
                        skills (skills tecniche, stringa),
                        soft_skills (stringa),
                        languages (stringa, solo in Italiano),
                        certifications (stringa),
                        education (stringa, solo in Italiano, possibili valori (solo uno): {", ".join(education_levels)})',
                        'Rispondi in formato JSON, senza testo extra, attieniti a questo esempio: 
                        {{"age": 30, "date_of_birth": "1993-05-12", "location": "Milano", "zip_code": "20100", 
                        "last_job": "Data Engineer", "second_last_job": "Developer", "third_last_job": "Intern", 
                        "skills": "Python, SQL, Java",
                        "soft_skills": "Precisione, Puntualità, Problem solving, Team Building, Gestione del tempo, Assistenza del cliente",
                        "languages": "Italiano, Inglese",
                        "certifications": "CISSP, EIPASS, ECDL, PEKIT, ESOL, MOS",
                        "education": "Diploma scuola superiore}}. ',
                        'Testo: ', description
                    )
                ) AS ner_json
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table_cleaned}
            --LIMIT 100
            )

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
            ["age", "date_of_birth", "location", "region", "country", "zip_code", "last_job", "second_last_job", "third_last_job", 
             "skills", "soft_skills", "languages", "certifications", "education"],
            [
                col("ner_json")["age"].cast("STRING"),
                col("ner_json")["date_of_birth"].cast("STRING"),
                col("ner_json")["location"].cast("STRING"),
                col("ner_json")["region"].cast("STRING"),
                col("ner_json")["country"].cast("STRING"),
                col("ner_json")["zip_code"].cast("STRING"), 
                col("ner_json")["last_job"].cast("STRING"),
                col("ner_json")["second_last_job"].cast("STRING"),
                col("ner_json")["third_last_job"].cast("STRING"),
                col("ner_json")["skills"].cast("STRING"),
                col("ner_json")["soft_skills"].cast("STRING"),
                col("ner_json")["languages"].cast("STRING"),
                col("ner_json")["certifications"].cast("STRING"),
                col("ner_json")["education"].cast("STRING"),
            ]
        )

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
        
        
        df = validate_string(df, "location")
        df = validate_string(df, "region")
        df = validate_string(df, "country")
        df = validate_string(df, "last_job")
        df = validate_string(df, "second_last_job")
        df = validate_string(df, "third_last_job")
        df = validate_string(df, "skills")
        df = validate_string(df, "soft_skills")
        df = validate_string(df, "languages")
        df = validate_string(df, "certifications")
        df = validate_string(df, "education")

        from snowflake.snowpark.functions import regexp_replace


        # makes sure zip_code is a valid formato
        df = df.with_column(
            "zip_code",
            when(
                col("zip_code").rlike("^[0-9]{5}$"),
                col("zip_code")
            ).otherwise(lit(None))
        )

        # makes sure age is a reasonable value
        df = df.with_column(
            "age",
            when(
                (col("age") != "nan") &
                (col("age").cast("INT").is_not_null()) &
                (col("age").cast("INT") >= 1) &
                (col("age").cast("INT") <= 150),
                col("age").cast("INT")
            ).otherwise(lit(None))
        )

        # makes sure date_of_birth is a valid date
        date_regex = (
            "^(" +
            # Months with 31 days
            f"(19[0-9][0-9]|20[0-9][0-9]|{today_string})-(01|03|05|07|08|10|12)-(0[1-9]|[12][0-9]|3[01])|" +
            # Months with 30 days
            f"(19[0-9][0-9]|20[0-9][0-9]|{today_string})-(04|06|09|11)-(0[1-9]|[12][0-9]|30)|" +
            # February (non-leap year logic: 1–28)
            f"(19[0-9][0-9]|20[0-9][0-9]|{today_string})-02-(0[1-9]|1[0-9]|2[0-8])" +
            ")$"
        )
        df = df.with_column(
            "date_of_birth",
            when(
                col("date_of_birth").rlike(date_regex),
                to_date(to_varchar(col("date_of_birth")), "YYYY-MM-DD"),
            ).otherwise(lit(None))
        )
        
        df = df.drop("ner_json")
        df = df.drop("description")

        logger.info(f"NER on {input_table_cleaned} table successful")

        return df

    def apply_chatwa_extraction(self, session):
        """
        Reads input table cleaned
        Performs CHATWA extraction to get CHATWA id from CANDIDATEID
        Function returns Snowflake dataframe
        """

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.output_table

        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Assegna True se il candidato ha risposto positivamente alla domanda "Sei disponibile a lavorare nel fine settimana?" altrimenti assegna False.
                        ️Rispondi solo con True o False. ',
                        'Testo: ', chatwa
                    )
                ) AS turno_preferenza_weekend
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table_cleaned}
            )

            """

        df = session.sql(query)


        from snowflake.snowpark.functions import col, when, lit, array_append, array_to_string

        df = df.with_column(
            "turno_preferenza",
            when(
                (lower(trim(col("turno_preferenza_weekend"))) == 'true') &
                (~array_to_string(col("turno_preferenza"), lit(",")).contains(lit("Fine Settimana"))),
                array_append(col("turno_preferenza"), lit("Fine Settimana"))
            ).otherwise(col("turno_preferenza"))
        )

        df = df.drop("chatwa")
        df = df.drop("turno_preferenza_weekend")

        return df
  
    
    def add_geo_info(self, session):
        """
        Reads candidate dataframe
        Reads italian cities dataframe
        Checks that zip_codes are valid
        Adds geo info (latitude, longitude)
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        output_table = self.config.output_table
        input_table_italian_cities = self.config.input_table_italian_cities
        
        candidates = session.table(f"{database}.{schema}.{output_table}")
        italian_cities = session.table(f"{database}.{schema}.{input_table_italian_cities}")

        # This avoids issues when the candidates table has already lat and long columns
        candidates = candidates.select(
            *[col(c) for c in candidates.columns if c.lower() not in {"latitude", "longitude", "province_ext"}]
        )
        
        valid_zips = [
            row[key]
            for row in italian_cities.select("zip").distinct().collect()
            for key in row.as_dict()
            if key.lower() == "zip"
        ]
        # Replace invalid ZIPs with None
        candidates = candidates.with_column(
            "zip_code",
            when(col("zip_code").isin(valid_zips), col("zip_code")).otherwise(lit(None))
            )

        # If zip_code is missing and location is available, get the zip_code from italian_cities dataframe
        df = candidates.join(
            italian_cities,
            lower(candidates["location"]) == lower(italian_cities["city_name"]),
            "left"
        )
        df = df.with_column(
            "zip_code",
            when(
                col("zip_code").is_null(), col("zip") 
            ).otherwise(col("zip_code"))
        )
        df = df.select(
            *[col(c.name) for c in candidates.schema.fields],
            )

        # Given the candidate location, get (lat, long) data from italian_cities
        df = df.join(
            italian_cities,
            lower(candidates["location"]) == lower(italian_cities["city_name"]),
            how="left"
        )
        df = df.select(
            *[col(c.name) for c in candidates.schema.fields],
            col("latitude"),
            col("longitude"),
            col("province_ext"),
            )
        
        df = validate_string(df, "province_ext")

        # Regex for latitude: -90 to 90 with optional decimals
        latitude_regex = r"^[-+]?([0-8]?\d(\.\d+)?|90(\.0+)?)$"
        # Regex for longitude: -180 to 180 with optional decimals
        longitude_regex = r"^[-+]?((1[0-7]\d|0?\d{1,2})(\.\d+)?|180(\.0+)?)$"

        # Make sure latitude and longiture are valid
        df = df.with_column(
            "latitude",
            when(
                col("latitude").rlike(latitude_regex),
                col("latitude").cast("FLOAT")
            ).otherwise(lit(None))
        ).with_column(
            "longitude",
            when(
                col("longitude").rlike(longitude_regex),
                col("longitude").cast("FLOAT")
            ).otherwise(lit(None))
        )

        df = df.with_column("distance_km", lit(999999))

        return df


    def write_table(self, df, table_name = 'output_table'):
        """
        Writes table
        Function returns nothing
        """
        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")

  

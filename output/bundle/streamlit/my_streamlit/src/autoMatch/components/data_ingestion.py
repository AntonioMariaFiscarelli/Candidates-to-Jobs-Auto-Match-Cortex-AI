from src.autoMatch import logger
import pandas as pd
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, dateadd, current_date, lit, coalesce, when, greatest, expr, split, listagg
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType



from src.autoMatch.entity.config_entity import DataIngestionConfig

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
        days_prior = self.config.days_prior
        desired_locations = self.config.desired_locations
        parttime_preferenza_perc = self.config.parttime_preferenza_perc

        #df = session.table(f"{database}.{schema}.{input_table}")
        #df = df.select([col(c) for c in columns])
        
        candidate_df = session.sql(f"""
                         SELECT {",".join(columns)} 
                         FROM {database}.{schema}.{input_table} 
                         """)
        
        candidate_df = candidate_df.filter(col("date_added") >= dateadd("day", lit(-days_prior), current_date()))
        candidate_df = candidate_df.with_column("candidateid", col("candidateid").cast("string"))
        
        joboti_df = session.sql("""
        WITH note AS (
            SELECT *
            FROM (
                SELECT personreferenceid, comments, action, load_datetimestamp, isdeleted,
                    ROW_NUMBER() OVER (
                        PARTITION BY noteid
                        ORDER BY load_datetimestamp DESC
                    ) AS row_num
                FROM aas_it_hist_prod.bullhorn.note
            )
            WHERE row_num = 1
            AND isdeleted = 0
        )
        SELECT personreferenceid, comments, load_datetimestamp
        FROM note
        WHERE action ILIKE '%joboti%'
        AND comments ILIKE '%Sei disponibile a lavorare nel fine settimana?%'
        """)

        agg_df = (
            joboti_df
            .group_by("PERSONREFERENCEID")
            .agg(
                listagg(col("COMMENTS"), " || ")
                    .within_group(col("LOAD_DATETIMESTAMP"))
                    .alias("CHATWA")
            )
            .with_column_renamed("PERSONREFERENCEID", "CANDIDATEID")
        )


        df = (
            candidate_df
            .join(
                agg_df,
                candidate_df["CANDIDATEID"] == agg_df["CANDIDATEID"],
                join_type="left",
                rsuffix="_right"
            )
            .select(candidate_df["*"], agg_df["CHATWA"])
        )

        #df = df.filter(col("CHATWA").is_not_null())
        
        case_parts = []
        for level in reversed(desired_locations):   # highest first
            case_parts.append(f"WHEN DESIRED_LOCATION ILIKE '%{level}%' THEN '{level}'")

        case_expr = "CASE " + " ".join(case_parts) + " ELSE NULL END"

        df = df.with_column(
            "DESIRED_LOCATION",
            expr(case_expr)
        )

        df = df.with_column(
            "turno_preferenza",
            split(col("turno_preferenza"), lit(","))
        )

        # Transform the dataframe
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            coalesce(col("PARTTIME_PREFERENZA_PERC"), lit("1%"))
        )

        # Build expressions: if STRING_COL contains 'v', return int(v), else 0
        exprs = [
            when(col("PARTTIME_PREFERENZA_PERC").contains(lit(v)), lit(int(v))).otherwise(lit(0))
            for v in parttime_preferenza_perc
        ]

        # Take the maximum across all expressions
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            greatest(*exprs)
        )

        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            when(col("PARTTIME_PREFERENZA_PERC") == 1, None).otherwise(col("PARTTIME_PREFERENZA_PERC"))
        )

        logger.info(f"Table {input_table} successfully read")

        return df
    
    def read_vacancy_table(self, session):
        """
        Reads input table
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table_vacancy = self.config.input_table_vacancy
        columns_vacancy = self.config.columns_vacancy
        days_prior = self.config.days_prior

        
        df = session.sql(f"""
                    WITH filtered_jc AS (
                        SELECT {",".join(columns_vacancy)} , isopen, status, datelastmodified
                        FROM {database}.{schema}.{input_table_vacancy} 
                        WHERE isopen = 1 AND status = 'Accepting Candidates'
                    )
                    SELECT 
                        DISTINCT jc.joborderid,
                        jc.status,
                        jc.datelastmodified,
                        jc.dateadded,
                        jc.jobtitle,
                        jc.citta AS location,
                        jc.regione AS region,
                        jc.salary AS salary_low,
                        jc.data_inizio_validita AS date_available,
                        COALESCE(CAST(jc.part_time_percent AS STRING), '') AS parttime_preferenza_perc,
                        COALESCE(jc.skill_list, '') AS skills,
                        COALESCE(jc.titoli_richiesti, '') AS education,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.DESCRIZIONE_USO_INTERNO), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS DESCRIZIONE_USO_INTERNO,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.TESTO_PUBBLICAZIONE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS TESTO_PUBBLICAZIONE,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.RICHIESTE_AGGIUNTIVE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS RICHIESTE_AGGIUNTIVE,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.REQUISITI), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS REQUISITI
                    FROM filtered_jc jc
                    LEFT JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.JOBSUBMISSION_CLEANED jsc
                        ON jc.joborderid = jsc.joborderid
                    WHERE 
                        jc.datelastmodified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                        OR jsc.data_ultimo_cambio_status >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                         """)
        
        logger.info(f"Table {input_table_vacancy} successfully read")

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
        df.columns = (
            df.columns
            .str.strip()
            .str.replace(" ", "_")
            .str.replace('"', '')
            .str.replace("'", '')
            .str.lower()
        )
        df = df[string_columns + numeric_columns]
        
        # Convert ZIP to string (preserve leading zeros)
        df["zip"] = df["zip"].apply(lambda x: str(int(x)).zfill(5) if pd.notnull(x) else None)
        
        # Convert string columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()

        # Convert latitude and longitude to float, handle NaNs
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove rows with missing city_name
        df = df[
            df["city_name"].notna() &  # Remove NaN and None
            (df["city_name"].str.strip() != "") &  # Remove empty and whitespace-only strings
            (df["city_name"].str.lower().str.strip() != "null") &  # Remove "NULL" string
            (df["city_name"].str.lower().str.strip() != "nan")  # Remove "nan" string
        ]

        #These are necessary in order to avoid columns names qith quotes (e.g. "city" instead of city)
        rows = [Row(**row) for row in df.to_dict(orient="records")]
        schema = StructType([
            StructField("unique_identifier", StringType()),
            StructField("city_name", StringType()),
            StructField("province", StringType()),
            StructField("province_ext", StringType()), 
            StructField("region", StringType()), 
            StructField("zip", StringType()),
            StructField("latitude", FloatType()),
            StructField("longitude", FloatType())
        ])

        logger.info(f"XLSX file containing italian cities successfully read")

        return session.create_dataframe(rows, schema=schema)
    
    def write_table(self, df, table_name = 'output_table'):
        """
        Writes table
        Function returns nothing
        """

        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")



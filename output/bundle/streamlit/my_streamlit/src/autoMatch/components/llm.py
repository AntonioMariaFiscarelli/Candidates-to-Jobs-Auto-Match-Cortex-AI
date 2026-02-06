from src.autoMatch import logger
from src.autoMatch.entity.config_entity import LLMConfig

from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col, lit, coalesce

from snowflake.snowpark.functions import col, trim, lower, length, parse_json, when, lit, trim, to_date, to_varchar
from snowflake.snowpark.types import StringType, BooleanType
from snowflake.snowpark.functions import udf

from src.autoMatch.utils.common import validate_string


import json
import difflib
        
class LLM:

    def __init__(self, config: LLMConfig):
        self.config = config

    def __normalize(self, s: str) -> str:
        return ''.join(ch for ch in s.lower() if ch.isalnum())

    def __find_synonyms_builtin(self, query: str, mapping: dict, cutoff: float = 0.8):
        """
        Return a flat list of synonym strings for keys that match the query.
        - Ignores underscores, hyphens, spaces and case.
        - Uses difflib.get_close_matches with a similarity cutoff (0..1).
        - Checks normalized keys and normalized synonym terms.
        """
        qn = self.__normalize(query)
        norm_index = {}   # normalized token -> original key
        norm_tokens = []  # list of normalized tokens to match against

        for key, synonyms in mapping.items():
            nk = self.__normalize(key)
            if nk not in norm_index:
                norm_index[nk] = key
                norm_tokens.append(nk)
            for term in synonyms:
                nt = self.__normalize(term)
                if nt not in norm_index:
                    norm_index[nt] = key
                    norm_tokens.append(nt)

        matches_keys = set()
        close = difflib.get_close_matches(qn, norm_tokens, n=20, cutoff=cutoff)
        for nk in close:
            matches_keys.add(norm_index[nk])

        # also accept direct substring matches (useful for multiword queries)
        for nk, orig_key in norm_index.items():
            if qn in nk or nk in qn:
                matches_keys.add(orig_key)

        # collect synonyms for matched keys, flatten and deduplicate preserving order
        result = []
        seen = set()
        for key in mapping:
            if key in matches_keys:
                for term in mapping[key]:
                    if term not in seen:
                        seen.add(term)
                        result.append(term)
        return result

    def create_prompt_row(self, role, 
                      skills, 
                      skills_soft,
                      skills_language,
                      skills_education,
                      skills_certifications):
        """
        Creates custom prompt based on potential candidates info and recruiter position info
        Returns prompt in string format
        """

        role_mappings = self.config.role_mappings

        similar_roles = self.__find_synonyms_builtin(role, role_mappings, cutoff=0.85)

        if similar_roles:
            role_mappings_string = f"{', '.join(similar_roles)}. "
        else:
            role_mappings_string = f"e ruoli simili"

        skills_text = f"""Dai +1 punto per ogni skill che il candidato ha tra le seguenti : {skills}. 
        (Ad esempio, se si ricerca la skill "SAP", si deve dare +1 a: "SAP", "software sap", "gestionale sap" ecc)
        """ if bool(skills) else ""
        languages_text = f"Dai un bonus ai candidati che conoscono le seguenti lingue: {', '.join(skills_language)}. " if bool(skills_language) else ""
        education_text = f"Dai un bonus ai candidati che hanno questo titolo di studio o superiore: {skills_education}. " if bool(skills_education) else ""
        certifications_text = f"Dai un bonus ai candidati che hanno le seguenti certificazioni: {skills_certifications}, anche se è all'interno di una lista di certificazioni. " if bool(skills_certifications) else ""


        
        prompt = f"""
        Dai un voto al candidato per il seguente ruolo: {role} ({role_mappings_string}).
        Dai sempre un voto più alto ai candidati i cui campi last_job, second_last_job o third_last_job contengono {role}, anche se all interno di una lista di ruoli.  
        Dai un voto più alto ai candidati che hanno coperto più volte questo ruolo. 
        Dai un voto più alto ai candidati che hanno coperto questo ruolo come last_job, rispetto a chi l ha coperto solo come second_last_job e third_last_job.
        Dai un voto basso ai candidati che non hanno esperienza regressa in ruoli simili. 
        Ad esempio 
            - last_job:{role}, second_last_job:{role} e third_last_job:{role} -> voto 100.
            - last_job:{role}, second_last_job:{role} e third_last_job:altro -> voto 85.
            - last_job:{role}, second_last_job:altro e third_last_job:{role} -> voto 70.
            - last_job:{role}, second_last_job:altro e third_last_job:altro -> voto 55.
            - last_job:altro, second_last_job:{role} e third_last_job:{role} -> voto 40.
            - last_job:altro, second_last_job:{role} e third_last_job:altro -> voto 25.
            - last_job:altro, second_last_job:altro e third_last_job:{role} -> voto 10. 
        {skills_text}
        {languages_text}
        {education_text}
        {certifications_text}
        Rispondi solo con un valore numerico da 0 a 100, dove 100 indica il candidato perfetto.
        """

        parts = [
            "'Ultimo lavoro: ', last_job",
            "'Penultimo lavoro: ', second_last_job",
            "'Terzultimo lavoro: ', third_last_job"
        ]

        if skills_text:
            parts.append("'Skills: ', skills")
        if languages_text:
            parts.append("'Languages: ', languages")
        if education_text:
            parts.append("'Education: ', education")
        if certifications_text:
            parts.append("'Certifications: ', certifications")

        # unisci solo le parti presenti con " | "
        text = ", ' | ', ".join(parts)
        text = "CONCAT(" + text + ")"

        #CONCAT('Ultimo lavoro: ', last_job, ' | Penultimo lavoro: ', second_last_job, ' | Terzultimo lavoro: ', third_last_job, ' | Skills: ', skills, ' | Languages: ', languages, ' | Education: ', education)


        logger.info("Prompt successfully created")

        return prompt, text 

    def call_ai_row(self, session, prompt, text):
        """
        Calls AI model on custom prompt
        Returns json response
        """
        llm_name = self.config.llm_name

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table

        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                    '{prompt.replace("'", "''")}',
                    'Testo: ', CONCAT('Ultimo lavoro: ', last_job, ' | Penultimo lavoro: ', second_last_job, ' | Terzultimo lavoro: ', third_last_job, 
                    ' | Skills: ', skills, ' | Languages: ', languages, ' | Education: ', education)   
                    )
                ) AS SCORE
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table}_APP
            --LIMIT 100
            )
            """
        #CONCAT('Ultimo lavoro: ', last_job, ' | Penultimo lavoro: ', second_last_job, ' | Terzultimo lavoro: ', third_last_job
        #CONCAT('Ultimo lavoro: ', last_job, ' | ', 'Penultimo lavoro: ', second_last_job, ' | ', 'Terzultimo lavoro: ', third_last_job)
        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                    '{prompt.replace("'", "''")}',
                    'Testo: ', {text}
                    )
                ) AS SCORE
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table}_APP
            --LIMIT 100
            )
            """
        

        logger.info(f"Running model {llm_name} on candidate data")

        df = session.sql(query)

        df = df.with_column(
            "SCORE",
            F.coalesce(F.try_cast(F.col("SCORE"), "int"), F.lit(0))
        )

        logger.info(f"Run completed")

        return df

    def create_prompt_row(self, role, 
                      skills_mand, 
                      skills_opt,
                      skills_languages_mand,
                      skills_languages_opt,
                      skills_education_mand,
                      skills_education_opt,
                      skills_certifications_mand,
                      skills_certifications_opt):
        """
        Creates custom prompt based recruiter requirements.
        It creates a prompt for each mandatory skill (asks the LLM to reply with True or False if the candidate has the spefic skill or not)
        It creates a prompt for each optional skill (asks the LLM to provide a score bases on the matching skills)

        Returns a prompt dictionary:
        prompts = {
            'NEW_COLUMN' : [prompt, columns],
            }
        NEW_COLUMN: new column created, bool for mandatory skills and numeric for optional skills
        prompt: prompt used for the Cortex AI request
        columns: candidate column(s) name (or combination) provided to Cortex AI
        """

        role_mappings = self.config.role_mappings

        # Looks up for similar roles to the one specified. i.e. "Magazziniere" will also include "carico scarico" and "warehouse worker"
        similar_roles = self.__find_synonyms_builtin(role, role_mappings, cutoff=0.85)

        if similar_roles:
            role_mappings_string = f"{', '.join(similar_roles)}. "
        else:
            role_mappings_string = f"e ruoli simili"

        
        prompt_jobs = f"""
        Dai un voto al candidato per il seguente ruolo: {role} ({role_mappings_string}).
        Dai sempre un voto più alto ai candidati i cui campi last_job, second_last_job o third_last_job contengono {role}, anche se all interno di una lista di ruoli.  
        Dai un voto più alto ai candidati che hanno coperto più volte questo ruolo. 
        Dai un voto più alto ai candidati che hanno coperto questo ruolo come last_job, rispetto a chi l ha coperto solo come second_last_job e third_last_job.
        Dai un voto basso ai candidati che non hanno esperienza regressa in ruoli simili. 
        Esattamente come mostrato di seguito: 
            - last_job:{role}, second_last_job:{role} e third_last_job:{role} -> voto 100.
            - last_job:{role}, second_last_job:{role} e third_last_job:altro -> voto 85.
            - last_job:{role}, second_last_job:altro e third_last_job:{role} -> voto 70.
            - last_job:{role}, second_last_job:altro e third_last_job:altro -> voto 55.
            - last_job:altro, second_last_job:{role} e third_last_job:{role} -> voto 40.
            - last_job:altro, second_last_job:{role} e third_last_job:altro -> voto 25.
            - last_job:altro, second_last_job:altro e third_last_job:{role} -> voto 0. 
        Rispondi solo con uno di questi valori numerici: 100, 85, 70, 55, 40, 25, 0.
        """

        jobs_column = [
            "'Ultimo lavoro: ', last_job",
            "'Penultimo lavoro: ', second_last_job",
            "'Terzultimo lavoro: ', third_last_job"
        ]
        jobs_column = ", ' | ', ".join(jobs_column)


        prompt_skills_mand = f"""
        Assegna True se il candidato ha tutte le seguenti skills (separate da virgola) : {skills_mand}, altrimenti False. 
        Se non ci sono skills disponibili, rispondi False.
        Ad esempio se si ricercano le seguenti due skills "SAP, Office":
            - skills: "Python, gestionale sap, SQL, microsoft office, Pyspark" -> True
            - skills: "Python, SQL, microsoft office, Pyspark" -> False
            - skills: "Python, gestionale sap, SQL, Pyspark" -> False
            - skills: "Python, SQL, Pyspark" -> False
        Rispondi solo con due possibili valori: True, False
        """ if bool(skills_mand) else ""

        prompt_skills_opt = f"""
        Assegna +1 punto per ogni skill che il candidato ha tra le seguenti (separate da virgola) : {skills_opt}. 
        Se non ci sono skills disponibili, restituisci 0.
        Ad esempio se si ricercano le seguenti due skills "SAP, Office":
            - skills: "Python, gestionale sap, SQL, microsoft office, Pyspark" -> 2
            - skills: "Python, SQL, microsoft office, Pyspark" -> 1
            - skills: "Python, gestionale sap, SQL, Pyspark" -> 1
            - skills: "Python, SQL, Pyspark" -> 0
        Rispondi solo con un valore numerico
        """ if bool(skills_opt) else ""

        prompt_languages_mand = f"""
        Assegna True se il candidato ha tutte le seguenti skills linguistiche (separate da virgola) : {", ".join(skills_languages_mand)}. Altrimenti False. 
        Se non ci sono languages disponibili, rispondi False.
        Rispondi solo con due possibili valori: True, False
        """ if bool(skills_languages_mand) else ""

        """
        Ad esempio se si ricercano le seguenti due lingue "Inglese, Francese, Spagnolo":
            - languages: "Italiano, Inglese, Tedesco, Francese, Spagnolo" -> True
            - languages: "Italiano, Inglese, Tedesco, Francese, Arabo" -> False
            - languages: "Italiano, Inglese, Tedesco, Spagnolo" -> False
            - languages: "Italiano, Tedesco, Francese" -> False
            - languages: "Italiano, Tedesco, Spagnolo" -> False
        """
        prompt_languages_opt = f"""
        Assegna +1 punto per ogni skill linguistica che il candidato ha tra le seguenti (separate da virgola) : {", ".join(skills_languages_opt)}. 
        Se non ci sono languages disponibili, restituisci 0.
        Ad esempio se si ricercano le seguenti due lingue "SAP, Office":
            - languages: "Italiano, Inglese, Tedesco, Francese" -> 2
            - languages: "Italiano, Inglese, Tedesco, Spagnolo" -> 1
            - languages: "Italiano, Tedesco, Francese" -> 1
            - languages: "Italiano, Tedesco, Spagnolo" -> 0
        Rispondi solo con un valore numerico
        """ if bool(skills_languages_opt) else ""

        prompt_education_mand = f"""
        Assegna True se il candidato ha almeno il seguento livello di educazione : {skills_education_mand}. Altrimenti False. 
        Se education non è disponibile, rispondi False.
        I livelli di education con il loro ranking sono i seguenti:
            - "Diploma scuola media": 1
            - "Diploma scuola superiore": 2
            - "Laurea triennale": 3
            - "Laurea specialistica": 4
            - "Dottorato": 5
        Ad esempio se si ricerca un candidato con livello di educazione "Laurea Triennale":
            - education: "Diploma scuola media" -> False
            - education: "Diploma scuola superiore" -> False
            - education: "Laurea triennale" -> True
            - education: "Laurea specialistica" -> True
            - education: "Dottorato" -> True
        Rispondi solo con due possibili valori: True, False
        """ if bool(skills_education_mand) else ""

        prompt_education_opt = f"""
        Assegna +1 punto per ogni livello di education extra rispetto a quello cercato: {skills_education_opt}.
        Se education non è disponibile, restituisci 0.
        I livelli di education con il loro ranking sono i seguenti: 
            - "Diploma scuola media": 1
            - "Diploma scuola superiore": 2
            - "Laurea triennale": 3
            - "Laurea specialistica": 4
            - "Dottorato": 5
        Ad esempio se si ricerca un candidato con livello di educazione "Laurea Triennale":
            - education: "Diploma scuola media" -> -2
            - education: "Diploma scuola superiore" -> -1
            - education: "Laurea triennale" -> 0
            - education: "Laurea specialistica" -> 1
            - education: "Dottorato" -> 2
        Un altro esempio, se si ricerca un candidato con livello di educazione "Diploma scuola superiore":
            - education: "Diploma scuola media" -> -1
            - education: "Diploma scuola superiore" -> 0
            - education: "Laurea triennale" -> 1
            - education: "Laurea specialistica" -> 2
            - education: "Dottorato" -> 3
        Un altro esempio, se si ricerca un candidato con livello di educazione "Laurea specialistica":
            - education: "Diploma scuola media" -> -3
            - education: "Diploma scuola superiore" -> -2
            - education: "Laurea triennale" -> -1
            - education: "Laurea specialistica" -> 0
            - education: "Dottorato" -> 1
        Rispondi solo con un valore numerico
        """ if bool(skills_education_opt) else ""

        prompt_certifications_mand = f"""
        Assegna True se il candidato ha tutte le seguenti certificazioni (separate da virgola) : {skills_certifications_mand}, altrimenti False. 
        Se non ci sono certificazioni disponibili, rispondi False.
        Ad esempio se si ricercano le seguenti due skills "CISM, Patente B":
            - skills: "CISM, Certificazione SAP, patente B, IELTS" -> True
            - skills: "CISM, Certificazione SAP, ISACA, IELTS" -> False
            - skills: "CISM, Certificazione SAP, patente muletto, IELTS" -> False
            - skills: "Comp TIA, Certificazione SAP, ISACA, IELTS" -> False
        Rispondi solo con due possibili valori: True, False
        """ if bool(skills_certifications_mand) else ""

        prompt_certifications_opt = f"""
        Assegna +1 punto per ogni certificazione che il candidato ha tra le seguenti (separate da virgola) : {skills_certifications_opt}. 
        Se non ci sono certificazioni disponibili, restituisci 0.
        Ad esempio se si ricercano le seguenti due certificazioni "CISM, Patente B":
            - skills: "CISM, Certificazione SAP, patente B, IELTS" -> 2
            - skills: "CISM, Certificazione SAP, ISACA, IELTS" -> 1
            - skills: "CISM, Certificazione SAP, patente muletto, IELTS" -> 1
            - skills: "Comp TIA, Certificazione SAP, patente muletto, IELTS" -> 0
        Rispondi solo con un valore numerico
        """ if bool(skills_certifications_opt) else ""


        prompts = {
            'SCORE_JOBS' : [prompt_jobs, jobs_column],
            'MAND_SKILLS': [prompt_skills_mand, 'skills'],
            'SCORE_SKILLS': [prompt_skills_opt, 'skills'],
            'MAND_LANGUAGES': [prompt_languages_mand, 'languages'],
            'SCORE_LANGUAGES': [prompt_languages_opt, 'languages'],
            'MAND_EDUCATION': [prompt_education_mand, 'education'],
            'SCORE_EDUCATION': [prompt_education_opt, 'education'],
            'MAND_CERTIFICATIONS': [prompt_certifications_mand, 'certifications'],
            'SCORE_CERTIFICATIONS': [prompt_certifications_opt, 'certifications']
            }

        logger.info("Prompt successfully created")

        return prompts
    

    def call_ai_row(self, session, prompts, candidates_df):
        """
        Queries the Vertex AI service based on the prompts
        Returns candidate dataframe with extracted information
        """
        llm_name = self.config.llm_name

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table

        #creates a Cortex AI call for each prompt provided
        ai_calls = []
        for key, (pt, field) in prompts.items():
            ai_call = f"""
            SNOWFLAKE.CORTEX.COMPLETE(
                'claude-4-sonnet',
                CONCAT(
                '{pt.replace("'", "''")}',
                'Testo: ', {field}
                )
            ) AS {key}, """ if bool(pt) else ""
            ai_calls.append(ai_call)
            
        ai_calls_sql = "".join(ai_calls)

        
        query = f"""
            SELECT
                *,
            {ai_calls_sql}            
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table}
            WHERE CANDIDATEID IN ({', '.join([str(cid) for cid in candidates_df.select('CANDIDATEID').to_pandas()['CANDIDATEID'].tolist()])})
            --LIMIT 100
            )
            """
        
        logger.info(f"Running model {llm_name} on candidate data")

        df = session.sql(query)


        # Combine each "MAND_" boolean column to filter out candidates that didn't match all requirements
        mand_cols = [c for c in df.schema.names if c.startswith("MAND_")]
        if(len(mand_cols) > 0):
            mand_expr = lit(True)
            for c in mand_cols:
                df = df.with_column(
                    c,
                    F.coalesce(F.sql_expr(f"TRY_TO_BOOLEAN({c})"), F.lit(False))
                    )
                mand_expr = mand_expr & (col(c) == lit(True))

            df = df.with_column("MAND", mand_expr)
            df = df.drop(*mand_cols)
        else:
            df = df.with_column("MAND", lit(True))

        df = df.filter(col("MAND") == True)

        # Sums up each individual score column to compute the final candidate score
        score_cols = [c for c in df.schema.names if c.startswith("SCORE_")]
        if(len(score_cols) > 0):
            score_expr = lit(0)
            for c in score_cols:
                df = df.with_column(
                    c,
                    F.coalesce(F.sql_expr(f"TRY_TO_NUMBER({c})"), F.lit(0))
                    )
                score_expr = score_expr + coalesce(col(c), lit(0))

            df = df.with_column(
                "SCORE",
                F.cast(F.round(score_expr), "int")
            )
            df = df.drop(*score_cols)
        else:
            df = df.with_column("SCORE", lit(0))

        df = df.filter(col("SCORE") > 0)
        logger.info(f"Run completed")

        return df

    def validate_json(self, df):
    
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
            ["turno_preferenza", "parttime_preferenza_perc", "skills", "languages", "education", "certifications"],
            [
                col("ner_json")["turno_preferenza"].cast("STRING"),
                col("ner_json")["parttime_preferenza_perc"].cast("STRING"),
                col("ner_json")["skills"].cast("STRING"),
                col("ner_json")["languages"].cast("STRING"), 
                col("ner_json")["education"].cast("STRING"),
                col("ner_json")["certifications"].cast("STRING"),
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
        
        
        df = validate_string(df, "turno_preferenza")
        df = validate_string(df, "parttime_preferenza_perc")
        df = validate_string(df, "skills")
        df = validate_string(df, "languages")
        df = validate_string(df, "education")
        df = validate_string(df, "certifications")


        # makes sure age is a reasonable value
        df = df.with_column(
            "parttime_preferenza_perc",
            when(
                (col("parttime_preferenza_perc") != "nan") &
                (col("parttime_preferenza_perc").cast("INT").is_not_null()) &
                (col("parttime_preferenza_perc").cast("INT") >= 0) &
                (col("parttime_preferenza_perc").cast("INT") <= 100),
                col("parttime_preferenza_perc").cast("INT")
            ).otherwise(lit(None))
        )

        df = df.drop("ner_json")
        df = df.drop("description")

        return df

    def extract_vacancy_info(self, session, vacancy_id):
        """

        """

        llm_name = self.config.llm_name

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table

        turno_preferenza = self.config.turno_preferenza
        education_levels = self.config.education_levels
        parttime_preferenza_perc = self.config.parttime_preferenza_perc

        open_texts = ["DESCRIZIONE_USO_INTERNO", "TESTO_PUBBLICAZIONE", "RICHIESTE_AGGIUNTIVE", "REQUISITI"]
        #all_texts = ["skills_list", "part_time_percent", "titoli_richiesti", "DESCRIZIONE_USO_INTERNO", "TESTO_PUBBLICAZIONE", "RICHIESTE_AGGIUNTIVE", "REQUISITI"]
        
        texts = ["skills", "parttime_preferenza_perc", "education", "DESCRIZIONE_USO_INTERNO", "TESTO_PUBBLICAZIONE", "RICHIESTE_AGGIUNTIVE", "REQUISITI"]
        labels = ["Skills richieste", "Percentuale part time", "Titoli di studio richiesti", "DESCRIZIONE USO INTERNO", "TESTO PUBBLICAZIONE", "RICHIESTE AGGIUNTIVE", "REQUISITI"]
        
        all_texts = []
        for label, text in zip(labels, texts):
            all_texts.append(f"""'{label}: ', {text} """)

        concat_text = f"""CONCAT( {", '| ', ".join(all_texts)}  )"""
        #concat_text = "CONCAT('skills: ', skills , 'descrizione uso interno: ', DESCRIZIONE_USO_INTERNO, ' | ', 'testo pubblicazione', TESTO_PUBBLICAZIONE)"

        query = f"""
            SELECT
                joborderid, dateadded, 
                jobtitle, 
                location, 
                region,
                country,
                salary_low, 
                date_available,
                {concat_text} as description,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Stai analizzando  una posizione lavorativa aperta, estrai i seguenti campi: 
                        turno_preferenza (turni richiesti per la posizione. stringa, possibili valori (anche multipli): {", ".join(turno_preferenza)}), 
                        parttime_preferenza_perc (percentuale di part time richiesta. stringa, possibili valori (solo uno): {", ".join(str(x) for x in parttime_preferenza_perc)}),
                        skills (skills tecniche richieste (no soft skills). stringa, ad esempio Python, guida muletto, gestione progetti ecc),
                        languages (lingue richieste, solo se richieste esplicitamente. stringa, ad esempio Inglese, Francese ecc )
                        education (titolo di studio richiesto. stringa, solo in Italiano. Possibili valori (solo uno): {", ".join(education_levels)}),
                        certifications (certificazioni richieste. stringa, ad esempio CISSP, EIPASS, ECDL, Patente B)',

                        'Rispondi in formato JSON, senza testo extra, attieniti a questo esempio: 
                        {{"turno_preferenza": "Pomeriggio, Notte" ,
                        "parttime_preferenza_perc": "30%",
                        "skills": "Python, SQL",
                        "languages": "Italiano, Inglese",
                        "education": "Diploma scuola superiore",
                        "certifications": "CISSP, EIPASS, ECDL"}}. ',

                        'Testo: ', {concat_text}
                    )
                ) AS ner_json
            FROM 
            (SELECT
                joborderid,
                dateadded,
                jobtitle,
                citta AS location,
                regione AS region,
                SNOWFLAKE.CORTEX.COMPLETE(
                'claude-4-sonnet',
                CONCAT(
                    'Estrai il paese dalla seguente città italiana. Rispondi solo con il nome del paese, in Italiano.',
                    'Città: ', citta,
                    'Regione: ', regione
                    )   
                ) AS country,
                --'Italia' AS country,
                salary AS salary_low,
                data_inizio_validita AS date_available,
                COALESCE(CAST(part_time_percent AS STRING), '') AS parttime_preferenza_perc,
                COALESCE(skill_list, '') AS skills,
                COALESCE(titoli_richiesti, '') AS education,
                COALESCE(REGEXP_REPLACE( REGEXP_REPLACE(TRIM(DESCRIZIONE_USO_INTERNO), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', '' ), '') AS DESCRIZIONE_USO_INTERNO,
                COALESCE(REGEXP_REPLACE( REGEXP_REPLACE(TRIM(TESTO_PUBBLICAZIONE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', '' ), '') AS TESTO_PUBBLICAZIONE,
                COALESCE(REGEXP_REPLACE( REGEXP_REPLACE(TRIM(RICHIESTE_AGGIUNTIVE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', '' ), '') AS RICHIESTE_AGGIUNTIVE,
                COALESCE(REGEXP_REPLACE( REGEXP_REPLACE(TRIM(REQUISITI), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', '' ), '') AS REQUISITI
            FROM {database}.{schema}.JOBORDER_CLEANED
            WHERE CAST(joborderid AS STRING) = '{vacancy_id}')
            """
        logger.info(f"Extracting vacancy info using model {llm_name}")


        materialize_query = f"""
        CREATE OR REPLACE TEMP TABLE TMP_VACANCY AS
        SELECT
            joborderid,
            dateadded,
            jobtitle,
            citta AS location,
            regione AS region,
            salary AS salary_low,
            data_inizio_validita AS date_available,
            COALESCE(CAST(part_time_percent AS STRING), '') AS parttime_preferenza_perc,
            COALESCE(skill_list, '') AS skills,
            COALESCE(titoli_richiesti, '') AS education,
            COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(DESCRIZIONE_USO_INTERNO), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS DESCRIZIONE_USO_INTERNO,
            COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(TESTO_PUBBLICAZIONE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS TESTO_PUBBLICAZIONE,
            COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(RICHIESTE_AGGIUNTIVE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS RICHIESTE_AGGIUNTIVE,
            COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REQUISITI), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS REQUISITI
        FROM {database}.{schema}.JOBORDER_CLEANED
        WHERE CAST(joborderid AS STRING) = '{vacancy_id}'
        """

        session.sql(materialize_query).collect()

        query = f"""
        SELECT
            joborderid,
            dateadded,
            jobtitle,
            location,
            region,
            SNOWFLAKE.CORTEX.COMPLETE(
                'claude-4-sonnet',
                CONCAT(
                    'Estrai il paese dalla seguente città italiana. Rispondi solo con il nome del paese, in Italiano.',
                    'Città: ', location,
                    'Regione: ', region
                )
            ) AS country,
            salary_low,
            date_available,
            {concat_text} AS description,
            SNOWFLAKE.CORTEX.COMPLETE(
                'claude-4-sonnet',
                CONCAT(
                    'Stai analizzando  una posizione lavorativa aperta, estrai i seguenti campi: 
                    turno_preferenza (turni richiesti per la posizione. stringa, possibili valori (anche multipli): {", ".join(turno_preferenza)}), 
                    parttime_preferenza_perc (percentuale di part time richiesta. stringa, possibili valori (solo uno): {", ".join(str(x) for x in parttime_preferenza_perc)}),
                    skills (skills tecniche richieste (no soft skills). stringa, ad esempio Python, guida muletto, gestione progetti ecc),
                    languages (lingue richieste, solo se richieste esplicitamente. stringa, ad esempio Inglese, Francese ecc )
                    education (titolo di studio richiesto. stringa, solo in Italiano. Possibili valori (solo uno): {", ".join(education_levels)}),
                    certifications (certificazioni richieste. stringa, ad esempio CISSP, EIPASS, ECDL, Patente B)',

                    'Rispondi in formato JSON, senza testo extra, attieniti a questo esempio: 
                    {{"turno_preferenza": "Pomeriggio, Notte" ,
                    "parttime_preferenza_perc": "30%",
                    "skills": "Python, SQL",
                    "languages": "Italiano, Inglese",
                    "education": "Diploma scuola superiore",
                    "certifications": "CISSP, EIPASS, ECDL"}}. ',

                    'Testo: ', {concat_text}
                )
            ) AS ner_json
        FROM TMP_VACANCY
        """


        df = session.sql(query)
        df = self.validate_json(df)

        return df
    
    def write_table(self, df, table_name = 'output_table'):
        """
        Writes table
        Function returns nothing
        """
        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")


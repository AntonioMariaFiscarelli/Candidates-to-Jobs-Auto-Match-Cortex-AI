from autoMatch import logger
from autoMatch.entity.config_entity import LLMConfig

import json
        
class LLM:
    def __init__(self, config: LLMConfig):
        self.config = config


    def create_prompt(self, role, skills, df_candidates, limit = 5):
        """
        Creates custom prompt based on potential candidates info and recruiter position info
        Returns prompt in string format
        """

        columns = self.config.columns  

        rows = df_candidates.collect()
        candidate_text = ""
        for row in rows:
            parts = [f"{columns[col]}:{row[col.upper()]}" for col in columns.keys()]
            candidate_text += " | ".join(parts) + "\n"

        skills_text = f"Skills desiderate: {skills}.\n" if skills else ""

        prompt = (
            f"Sei un selezionatore di personale.\n"
            f"Ruolo ricercato: {role}; dai priorita candidati che hanno avuto ruoli simili, "
            f"Non tenere in considerazione eta elocation\n"
            f"Skills richieste: {skills_text}; dai priorita a chi ha le skills piu rilevanti per il ruolo ricercato."
            f"Valuta i candidati seguenti e scegli i {limit} migliori. Dai ad ognuno un punteggio.\n"
            f"Candidati:\n{candidate_text}"
            f"Restituisci un JSON con: id, nome (candidateid), età, location, posizioni passate rilevanti, skills, motivazione, punteggio.\n\n"
        )

        logger.info("Prompt successfully created")

        return prompt

    def call_ai(self, session, prompt):
        """
        Calls AI model on custom prompt
        Returns json response
        """
        llm_name = self.config.llm_name

        query = f"""
        SELECT AI_COMPLETE(
            '{llm_name}',
            '{prompt.replace("'", "''")}'
        ) AS response
        """

        logger.info(f"Running model {llm_name} on candidate data")

        cur = session.connection.cursor()
        cur.execute(query)
        row = cur.fetchone()
        response = row[0]

        logger.info(f"Run completed")

        try:
            return json.loads(response)
        except:
            return [{"errore": response}]


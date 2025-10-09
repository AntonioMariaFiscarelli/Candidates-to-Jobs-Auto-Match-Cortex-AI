# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from autoMatch.utils.snowflake_utils import get_snowpark_session

import pandas as pd
import json

# Get the current credentials
#session = get_active_session()
@st.cache_resource
def get_snowpark_session_streamlit():
    return get_snowpark_session()


# ✅ This will only run once per session
session = get_snowpark_session_streamlit()



# 1 - prompt function definition
# -------------------------------
def build_ai_prompt(role, location, max_age, skills, df_candidates):
    candidate_text = ""
    for _, row in df_candidates.iterrows():
        candidate_text += (
            f"ID:{row['CANDIDATEID']} | Età:{row['AGE']} | Location:{row['LOCATION']} | "
            f"Ultimi lavori: {row['LAST_JOB']}, {row['SECOND_LAST_JOB']}, {row['THIRD_LAST_JOB']} | "
            f"Skills: {row['SKILLS']}\n"
        )
    
    skills_text = f"Skills desiderate: {skills}.\n" if skills else ""
    
    prompt = (
        f"Sei un selezionatore di personale.\n"
        f"Ruolo ricercato: {role}\n"
        f"Vincoli: location = {location}, età massima = {max_age}.\n"
        f"{skills_text}"
        f"Valuta i candidati seguenti e scegli i 5 migliori, decidendo autonomamente quali caratteristiche siano più rilevanti per questo ruolo.\n"
        f"Tieni in considerazione anche la location: il candidato deve trovarsi nella stessa provincia dell'annuncio di lavoro.\n"
        f"Restituisci un JSON con: id, nome (candidateid), età, location, skills, motivazione, punteggio.\n\n"
        f"Candidati:\n{candidate_text}"
    )
    return prompt


# 2 - AI function call
# -------------------------------
def call_ai(session, prompt):
    query = f"""
    SELECT AI_COMPLETE(
        'mistral-large',
        '{prompt.replace("'", "''")}'
    ) AS response
    """
    cur = session.connection.cursor()
    cur.execute(query)
    row = cur.fetchone()
    response = row[0]

    try:
        return json.loads(response)
    except:
        # if crash: shows raw text instead of json
        return [{"errore": response}]



st.title("🔎 Automatch")
st.markdown("Inserisci i criteri di ricerca per trovare i candidati ideali.")

st.subheader("Filtri di ricerca")

role = st.text_input("Ruolo ricercato")
location = st.text_input("Location")
max_age = st.number_input("Età massima", min_value=18, max_value=70, value=40)
skills = st.text_area("Skills richieste (separate da virgola)")


if st.button("Cerca candidati"):
    with st.spinner("Recupero candidati e generazione ranking AI..."):

        query = f"""
            SELECT CANDIDATEID, AGE, LOCATION, LAST_JOB, SECOND_LAST_JOB, THIRD_LAST_JOB, SKILLS
            FROM IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_CANDIDATE_FEATURES
            WHERE AGE <= {max_age}       
        """

        if location.strip():
            query += f"AND LOCATION ILIKE '%{location.strip()}%'"

        if skills.strip():
            skills_list = [s.strip() for s in skills.split(',')]
            for skill in skills_list:
                query += f" AND SKILLS ILIKE '%{skill}%'"

        df_candidates = session.sql(query).to_pandas()

        if df_candidates.empty:
            st.warning("Nessun candidato trovato")
        else:
            # --- Costruzione prompt e chiamata AI ---
            prompt = build_ai_prompt(role, location, max_age, skills, df_candidates)
            top_candidates = call_ai(session, prompt)

            st.markdown(f"*Top candidates:* {top_candidates}")


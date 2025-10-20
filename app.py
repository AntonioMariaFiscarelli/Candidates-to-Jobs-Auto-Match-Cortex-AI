# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from autoMatch.utils.snowflake_utils import get_snowpark_session

import pandas as pd
import numpy as np
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
def build_ai_prompt(role, location, age, skills, df_candidates):
    candidate_text = ""
    for _, row in df_candidates.iterrows():
        candidate_text += (
            f"ID:{row['candidateid']} | Età:{row['age']} | Location:{row['location']} | "
            f"ID:{row['candidateid']} |"
            f"Ultimi lavori: {row['last_job']}, {row['second_last_job']}, {row['third_last_job']} | "
            f"Skills: {row['skills']}\n"
        )
    
    skills_text = f"Skills desiderate: {skills}.\n" if skills else ""
    
    prompt = (
        f"Sei un selezionatore di personale.\n"
        f"Ruolo ricercato: {role}; dai priorita candidati che hanno avuto ruoli simili, e mostra se ha coperto esattamente la stessa posizione.\n"
        #f"Vincoli: location = {location}, età massima = {max_age}.\n"
        f"Skills richieste: {skills_text}; dai priorita a chi ha le skills piu rilevanti per il ruolo ricercato."
        f"Valuta i candidati seguenti e scegli i 5 migliori. Dai ad ognuno un punteggio.\n"
        #f"Tieni in considerazione anche la location: il candidato deve trovarsi nella stessa provincia dell'annuncio di lavoro.\n"
        f"Restituisci un JSON con: id, nome (candidateid), età, location, posizioni passate rilevanti, skills, motivazione, punteggio.\n\n"
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
skills = st.text_area("Skills essenziali (separate da virgola)")
skills_optional = st.text_area("Skills opzionali (separate da virgola)")

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    lat1_rad, lon1_rad = np.radians(lat1), np.radians(lon1)
    lat2_rad, lon2_rad = np.radians(lat2), np.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c

if st.button("Cerca candidati"):
    with st.spinner("Recupero candidati e generazione ranking AI..."):

        query = f"""
            SELECT CANDIDATEID, AGE, LOCATION, LAST_JOB, SECOND_LAST_JOB, THIRD_LAST_JOB, SKILLS, LATITUDE, LONGITUDE
            FROM IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_CANDIDATE_FEATURES
            WHERE AGE <= {max_age}       
        """
        if skills.strip():
            skills_list = [s.strip().lower() for s in skills.split(',')]
            for skill in skills_list:
                query += f" AND SKILLS ILIKE '%{skill}%'"

        df_candidates = session.sql(query).to_pandas()
        df_candidates.columns = df_candidates.columns.str.lower()


        location_coords = session.sql(f"""
                                      SELECT LATITUDE, LONGITUDE
                                      FROM IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_ITALIAN_CITIES
                                      WHERE LOWER(TRIM(city_name)) = LOWER(TRIM('{location}'))
                                      """).collect()
        if not location_coords:
            st.warning("Location not found.")
        else:
            print(location_coords[0].as_dict().keys())
            location_dict = location_coords[0].as_dict()
            lat_ref = location_dict.get("LATITUDE")
            lon_ref = location_dict.get("LONGITUDE")


            df_candidates["distance_km"] = haversine(
                lat_ref,
                lon_ref,
                df_candidates["latitude"],
                df_candidates["longitude"]
            )
            df_candidates = df_candidates[df_candidates["distance_km"] <= 20]
            print(df_candidates.head())

            columns_to_drop = ["latitude", "longitude"]
            df_candidates = df_candidates.drop(columns=columns_to_drop, axis=1)


        if df_candidates.empty:
            st.warning("Nessun candidato trovato")
        else:
            # --- Costruzione prompt e chiamata AI ---
            prompt = build_ai_prompt(role, location, max_age, skills + ", " + skills_optional, df_candidates)
            top_candidates = call_ai(session, prompt)

            st.markdown(f"*Top candidates:* {top_candidates}")


# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from autoMatch.utils.snowflake_utils import get_snowpark_session
from src.autoMatch.utils.common import haversine
from snowflake.snowpark import functions as F

from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.llm import LLM



# Get the current credentials
#session = get_active_session()
@st.cache_resource
def get_snowpark_session_streamlit():
    return get_snowpark_session()


@st.cache_resource
def get_LLM():
    config = ConfigurationManager()
    llm_config = config.get_llm_config()
    llm = LLM(config=llm_config)
    return llm

def init_role_input():
    st.session_state.role = st.text_input("Ruolo ricercato", value="Data Scientist")

def init_location_input():
    st.session_state.location = st.text_input("Location", value="Torino")

def init_max_distance_input():
    st.session_state.max_distance = st.number_input("Distanza massima in KM", value=20)

def init_max_age_input():
    st.session_state.max_age = st.number_input("Età massima", min_value=18, value=40, step=1)

def init_skills_input():
    st.session_state.skills = st.text_area("Skills essenziali (separate da virgola)", value="Python")

def init_skills_optional_input():
    st.session_state.skills_optional = st.text_area("Skills opzionali (separate da virgola)", value="Kafka, AirFlow, ETL")

def init_limit_input():
    st.session_state.limit = st.number_input("Numero di candidati", min_value=1, value=5)



st.title("🔎 Automatch")
st.markdown("Inserisci i criteri di ricerca per trovare i candidati ideali.")

st.subheader("Filtri di ricerca")

session = get_snowpark_session_streamlit()
llm = get_LLM()

init_role_input()
init_location_input()
init_max_distance_input()
init_max_age_input()
init_skills_input()
init_skills_optional_input()
init_limit_input()


if st.button("Cerca candidati"):
    with st.spinner("Recupero candidati e generazione ranking AI..."):

        db = llm.config.database
        schema = llm.config.schema
        table = llm.config.input_table

        df_candidates = session.table(f"{db}.{schema}.{table}")

        # Age filter
        if st.session_state.max_age:
            df_candidates = df_candidates.filter(F.col("AGE") <= st.session_state.max_age)

        # Skills filter
        if st.session_state.skills.strip():
            skills_list = [s.strip().lower() for s in st.session_state.skills.split(",")]
            for skill in skills_list:
                df_candidates = df_candidates.filter(F.lower(F.col("SKILLS")).like(f"%{skill}%"))

        # --- Get reference location ---
        df_location = (
            session.table("IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_ITALIAN_CITIES")
            .filter(F.lower(F.trim(F.col("CITY_NAME"))) == st.session_state.location.strip().lower())
        )
        location_coords = df_location.collect()

        if not location_coords:
            st.warning("Location non trovata.")
        else:
            location_dict = location_coords[0].as_dict()
            lat_ref = location_dict.get("LATITUDE")
            lon_ref = location_dict.get("LONGITUDE")

            df_candidates = df_candidates.with_column("distance_km", haversine(lat_ref, lon_ref))
            df_candidates = df_candidates.filter(F.col("distance_km") <= st.session_state.max_distance)

            df_candidates = df_candidates.drop("LATITUDE", "LONGITUDE")


        # Count rows in the Snowpark DataFrame
        candidate_count = df_candidates.count()

        if candidate_count == 0:
            st.warning("Nessun candidato trovato")
        else:
            prompt = llm.create_prompt(
                st.session_state.role,
                st.session_state.skills + ", " + st.session_state.skills_optional,
                df_candidates,
                {st.session_state.limit}
            )
            top_candidates = llm.call_ai(session, prompt)

            st.markdown(
                f"*Top {st.session_state.limit} candidates among {candidate_count}:* {top_candidates}"
            )
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

session = get_snowpark_session_streamlit()
llm = get_LLM()

#TITOLI_STUDIO_IT = [
#    "Diploma scuola media", "Diploma scuola superiore", "Laurea triennale", "Laurea specialistica", "Dottorato"
#]
education_levels = [""] + llm.config.education_levels


def init_role_input():
    st.session_state.role = st.text_input("Ruolo ricercato", value="Magazziniere")

def init_location_input():
    st.session_state.location = st.text_input("Location", value="Bari")

def init_max_distance_input():
    st.session_state.max_distance = st.number_input("Distanza massima in KM", value=20)

def init_max_age_input():
    st.session_state.max_age = st.number_input("Età massima", min_value=18, value=40, step=1)

def init_skills_input():
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.skills_mand = st.text_area(
            "Skills tecniche essenziali (separate da virgola)",
            value="",#"Muletto",
            key="skills_mand_text_area"
        )

    # metti la seconda text_area nella seconda colonna
    with col2:
        st.session_state.skills_opt = st.text_area(
            "Skills tecniche bonus (separate da virgola)",
            value="",#"Excel",
            key="skills_opt_text_area"
        )

def init_skills_soft_input():
    st.session_state.skills_soft = st.text_area("Skills soft (separate da virgola)", value="")

LINGUE_IT = [
    "Inglese", "Italiano", "Spagnolo", "Francese", "Tedesco",
    "Portoghese", "Cinese", "Giapponese", "Coreano", "Russo",
    "Arabo", "Olandese", "Svedese", "Polacco", "Turco", "Urdu", "Bangalese"
]

def init_skills_languages_input():
    st.session_state.setdefault("skills_languages_selected_mand", [])
    st.session_state.setdefault("skills_languages_selected_opt", [])

    col1, col2 = st.columns(2)
    with col1:
        st.multiselect(
            "Seleziona lingue obbligatorie",
            options=LINGUE_IT,
            key="skills_languages_selected_mand"
        )

    with col2:
        st.multiselect(
            "Seleziona lingue bonus",
            options=LINGUE_IT,
            key="skills_languages_selected_opt"
        )

    st.session_state.skills_languages_mand = st.session_state["skills_languages_selected_mand"]
    st.session_state.skills_languages_opt = st.session_state["skills_languages_selected_opt"]


def init_skills_education_input():

    st.session_state.setdefault("skills_education_selected_mand",  education_levels[0])
    st.session_state.setdefault("skills_education_selected_opt", education_levels[0])

    # layout: multiselect larga, checkbox stretta a destra
    col1, col2 = st.columns(2)
    with col1:
        st.selectbox(
            "Seleziona il titolo di studio minimo",
            options=education_levels,
            key="skills_education_selected_mand"
        )
    with col2:
        st.selectbox(
            "Seleziona titolo di studio bonus",
            options=education_levels,
            key="skills_education_selected_opt"
        )

    st.session_state.skills_education_mand = st.session_state["skills_education_selected_mand"]
    st.session_state.skills_education_opt = st.session_state["skills_education_selected_opt"]# if ed_opt != education_levels[0] else ""

def init_skills_certifications_input():
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.skills_certifications_mand = st.text_area(
            "Certificazioni essenziali (separate da virgola)",
            value="",#"Muletto",
            key="skills_certifications_mand_text_area"
        )

    with col2:
        st.session_state.skills_certifications_opt = st.text_area(
            "Certificazioni bonus (separate da virgola)",
            value="",#"Excel",
            key="skills_certifications_opt_text_area"
        )

def init_limit_input():
    st.session_state.limit = st.number_input("Numero di candidati", min_value=1, value=5)



st.title("🔎 Automatch")
st.markdown("Inserisci i criteri di ricerca per trovare i candidati ideali.")

st.subheader("Filtri di ricerca")



init_role_input()
init_location_input()
init_max_distance_input()
init_max_age_input()
init_skills_input()
#init_skills_soft_input()
st.write("")
init_skills_languages_input()
init_skills_education_input()
init_skills_certifications_input()
#init_limit_input()


if st.button("Cerca candidati"):
    with st.spinner("Recupero candidati e generazione ranking AI..."):

        db = llm.config.database
        schema = llm.config.schema
        table = llm.config.input_table

        columns = llm.config.columns  


        df_candidates = session.table(f"{db}.{schema}.{table}")

        provinces = ['Foggia', 'Bari', 'Taranto', 'Lecce', 'Brindisi']

        df_candidates = df_candidates.filter(F.col("PROVINCE_EXT").isin(provinces))


        # Age filter
        if st.session_state.max_age:
            df_candidates = df_candidates.filter(
                (F.col("AGE") <= st.session_state.max_age) | F.col("AGE").is_null()
            )

        # Skills filter
        if(False):
            if st.session_state.skills.strip():
                skills_list = [s.strip().lower() for s in st.session_state.skills.split(",")]
                for skill in skills_list:
                    df_candidates = df_candidates.filter(F.lower(F.col("SKILLS")).like(f"%{skill}%"))
            
            if st.session_state.skills_languages_mand and st.session_state.skills_languages:
                #skills_list = [s.strip().lower() for s in st.session_state.skills_languages.split(",")]
                skills_list = [s.lower() for s in st.session_state.skills_languages]
                for skill in skills_list:
                    df_candidates = df_candidates.filter(F.lower(F.col("LANGUAGES")).like(f"%{skill}%"))

            if st.session_state.skills_education_mand and st.session_state.skills_education:
                st.markdown(st.session_state.skills_education)
                mapping = {level: i+1 for i, level in enumerate(education_levels)}

                case_expr = "CASE "
                for k, v in mapping.items():
                    case_expr += f"WHEN lower(trim(EDUCATION)) = '{k.lower()}' THEN {v} "
                case_expr += "ELSE NULL END"

                df_candidates = df_candidates.with_column("edu_rank", F.expr(case_expr))

                threshold_pos = mapping[st.session_state.skills_education]
                df_candidates = df_candidates.filter(F.col("edu_rank") >= F.lit(threshold_pos)) 

            # Skills filter
            if st.session_state.skills_certifications_mand and st.session_state.skills_certifications.strip():
                skills_list = [s.strip().lower() for s in st.session_state.skills_certifications.split(",")]
                for skill in skills_list:
                    df_candidates = df_candidates.filter(F.lower(F.col("CERTIFICATIONS")).like(f"%{skill}%"))

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
        
        min_education_mand = True if st.session_state.skills_education_mand == education_levels[0] else False
        min_education_opt = True if st.session_state.skills_education_opt == education_levels[0] else False

        if candidate_count == 0:
            st.warning("Nessun candidato trovato")
        else:
            prompts = llm.create_prompt_row(
                st.session_state.role,
                st.session_state.skills_mand,
                st.session_state.skills_opt,
                st.session_state.skills_languages_mand,
                st.session_state.skills_languages_opt,
                st.session_state.skills_education_mand,
                st.session_state.skills_education_opt,
                st.session_state.skills_certifications_mand,
                st.session_state.skills_certifications_opt
            )

            df_candidates.write.save_as_table(f"{db}.{schema}.{table}_APP", mode="overwrite")

            if(False):
                st.text(
                    f"{prompt}"
                    )
                for key, (pt, field) in prompts.items():
                    st.text(
                        f"{pt}"
                        )

            cols = ["candidateid", "age", "location", "last_job", "second_last_job", "third_last_job"]
            #cols.append("score_job")
            if bool(st.session_state.skills_mand):
                cols.append("skills")
                #cols.append("mand_skills")
            if bool(st.session_state.skills_opt):
                if "skills" not in cols: cols.append("skills")
                #cols.append("score_skills")
            if bool(st.session_state.skills_languages_mand):
                cols.append("languages")
                #cols.append("mand_languages")
            if bool(st.session_state.skills_languages_opt):
                if "languages" not in cols: cols.append("languages")
                #cols.append("score_languages")
            if bool(st.session_state.skills_education_mand):
                cols.append("education")
                #cols.append("mand_education")
            if bool(st.session_state.skills_education_opt):
                if "education" not in cols: cols.append("education")
                #cols.append("score_education")
            if bool(st.session_state.skills_certifications_mand):
                cols.append("certifications")
                #cols.append("mand_certifications")
            if bool(st.session_state.skills_certifications_opt):
                if "certifications" not in cols: cols.append("certifications")
                #cols.append("score_certifications")
            #cols.append("mand")
            cols.append("score")

            top_candidates = llm.call_ai_row(session, prompts)
            #.text(top_candidates)
            top_candidates = top_candidates.sort(F.col("SCORE").desc()).select(cols)#(columns)

            top_candidates = top_candidates.to_pandas()
            top_candidates["AGE"] = top_candidates["AGE"].astype("Int64")
            st.markdown(
                f"*Top candidates:*"
            )
            st.table(
                top_candidates
            )
            #st.dataframe(top_candidates, use_container_width=True)

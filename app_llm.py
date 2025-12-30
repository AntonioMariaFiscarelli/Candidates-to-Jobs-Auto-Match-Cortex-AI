# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from autoMatch.utils.snowflake_utils import get_snowpark_session
from datetime import date
from src.autoMatch.utils.common import haversine
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col, lit, concat, to_varchar, expr, count_distinct, lower, trim

from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.llm import LLM



st.set_page_config(layout="wide")

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

desired_locations = [""] + llm.config.desired_locations

turno_preferenza = [""] + llm.config.turno_preferenza

parttime_preferenza_perc = [0] + llm.config.parttime_preferenza_perc # [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

def init_role_input():
    st.session_state.role = st.text_input("Ruolo ricercato", value="Magazziniere", 
                                          help="Esempio: Magazziniere")

def init_location_input():
    st.session_state.location = st.text_input("Location", value="Bari", 
                                              help="Città di ricerca. Esempio: Bari")

def init_max_distance_input():
    st.session_state.max_distance = st.number_input("Distanza massima in KM", value=20, 
                                                    help="""Distanza massima di ricerca rispetto alla città di riferimento. 
                                                    Tutti i candidati al di fuori del raggio specificato verrano scartati
                                                    """)

def init_max_age_input():
    st.session_state.max_age = st.number_input("Età massima", min_value=18, value=40, step=1, help="""Età massima. 
                                               Tutti i candidati di età superiore verrano scartati""")

def bullhorn_fields_input():
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.session_state.max_ral = st.number_input("RAL attuale", min_value=0, step=1, help="""RAL attuale. 
                                            Tutti i candidati con RAL superiore a quella specificate verranno scartati""")
    with col2:
        st.session_state.date_available = st.date_input(
            "Disponibile da",
            value=None,#date.today(),   # default to today
            help="""Data in cui il candidato è disponibile a iniziare. 
                    I candidati con disponibilità successiva verranno scartati."""
        ) 

    with col3:
        st.session_state.will_relocate = st.checkbox(
            "Disponibile al trasferimento",
            help="""Se selezionato, verranno mostrati solo i candidati disponibili al trasferimento."""
        )
        #if st.button("Resetta data a oggi"):
        #    st.session_state.date_available = date.today()
    
    st.session_state.setdefault("desired_locations",  desired_locations[0])
    with col4:
        st.selectbox(
            "Disponibilità geografica",
            options=desired_locations,
            key="desired_locations",
            help="""I candidati che non sono disponibili nelle zone specificate verranno scartati"""
        )

    st.session_state.setdefault("turno_preferenza", [])
    with col5:
        st.multiselect(
            "Seleziona turni",
            options=turno_preferenza,
            key="turno_preferenza",
            help="""I candidati che non sono disponibili nei turni specificati verranno scartati"""
        )

    with col6:
        st.selectbox(
            "Seleziona % part-time",
            options=parttime_preferenza_perc,
            key="parttime_preferenza_perc",
            help="""I candidati con percentuale preferenza part-time inferiore a quella specificata verranno scartati"""
        )

def init_max_ral_input():
    st.session_state.max_ral = st.number_input("RAL attuale", min_value=0, step=1, help="""RAL attuale. 
                                               Tutti i candidati con RAL superiore a quella specificate verranno scartati""")

def init_date_available_input():
    st.session_state.date_available = st.date_input(
        "Disponibile da",
        value=date.today(),   # default to today
        help="""Data in cui il candidato è disponibile a iniziare. 
                I candidati con disponibilità successiva verranno scartati."""
    )


def init_skills_input():
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.skills_mand = st.text_area(
            "Skills tecniche essenziali (separate da virgola)",
            value="",#"Muletto",
            key="skills_mand_text_area",
            help="""Esempio: muletto, pacchetto office, gestionale inventario. 
            I candidati non in possesso di tutte le skills richieste verrano scartati"""
        )

    # metti la seconda text_area nella seconda colonna
    with col2:
        st.session_state.skills_opt = st.text_area(
            "Skills tecniche bonus (separate da virgola)",
            value="",#"Excel",
            key="skills_opt_text_area",
            help="""Esempio: muletto, pacchetto office, gestionale inventario. 
            Un bonus sarà assegnato per ogni skill corrispondente del candidato"""
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
            key="skills_languages_selected_mand",
            help="""I candidati che non conoscono le lingue richieste verrano scartati"""
        )

    with col2:
        st.multiselect(
            "Seleziona lingue bonus",
            options=LINGUE_IT,
            key="skills_languages_selected_opt",
            help="""Un bonus sarà assegnato per ogni lingua conosciuta dal candidato tra quelle richieste"""
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
            key="skills_education_selected_mand",
            help="I candidati che non possiedono un titolo di studio equivalente o superiore verranno scartati"""
        )
    with col2:
        st.selectbox(
            "Seleziona titolo di studio bonus",
            options=education_levels,
            key="skills_education_selected_opt",
            help="Un bonus sarà assegnato per ogni titolo di studio del candidato che risulti superiore a quello richiesto"
        )

    st.session_state.skills_education_mand = st.session_state["skills_education_selected_mand"]
    st.session_state.skills_education_opt = st.session_state["skills_education_selected_opt"]# if ed_opt != education_levels[0] else ""

def init_skills_certifications_input():
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.skills_certifications_mand = st.text_area(
            "Certificazioni essenziali (separate da virgola)",
            value="",#"Muletto",
            key="skills_certifications_mand_text_area",
            help="""Esempio: patente B, ECTL. 
            I candidati che non possiedono le certificazioni richieste verranno scartati"""
        )

    with col2:
        st.session_state.skills_certifications_opt = st.text_area(
            "Certificazioni bonus (separate da virgola)",
            value="",#"Excel",
            key="skills_certifications_opt_text_area",
            help="""Esempio: patente B, ECTL. 
            Un bonus sarà assegnato per ogni certificazione corrispondente del candidato
            """
        )

def init_limit_input():
    st.session_state.limit = st.number_input("Numero di candidati", min_value=1, value=5)

def columns_to_show():
    cols_default = ["candidateid", "first_name", "last_name", "location", "url"]
    #cols.append("score_job")

    if st.session_state.max_age:
        cols_default.append("age")

    cols_default.extend(["last_job", "second_last_job", "third_last_job"])

    cols = cols_default
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

    if bool(st.session_state.max_ral):
        cols.append("salary_low")
    if bool(st.session_state.date_available):
        cols.append("date_available")
    if bool(st.session_state.will_relocate):
        cols.append("will_relocate")
    if bool(st.session_state.desired_locations):
        cols.append("desired_locations")
    if bool(st.session_state.turno_preferenza):
        cols.append("turno_preferenza")
    if bool(st.session_state.parttime_preferenza_perc):
        cols.append("parttime_preferenza_perc")
    cols.append("score")

    return cols_default, cols



st.title("🔎 Automatch")
st.markdown("Inserisci i criteri di ricerca per trovare i candidati ideali.")

st.subheader("Filtri di ricerca")



init_role_input()
init_location_input()
init_max_distance_input()
init_max_age_input()
bullhorn_fields_input()
#init_max_ral_input()
#init_date_available_input()
init_skills_input()
#init_skills_soft_input()
st.write("")
init_skills_languages_input()
init_skills_education_input()
init_skills_certifications_input()
#init_limit_input()

def filter_candidates():

    df_candidates = session.table(f"{db}.{schema}.{table}")

    provinces = ['Foggia', 'Bari', 'Andria', 'Trani', 'BAT', 'Taranto', 'Lecce', 'Brindisi']
    df_candidates = df_candidates.filter(col("PROVINCE_EXT").isin(provinces))

    if st.session_state.max_age:
        df_candidates = df_candidates.filter(
            (col("AGE") <= st.session_state.max_age) | col("AGE").is_null()
        )

    if st.session_state.max_ral:
        df_candidates = df_candidates.filter(
            (col("SALARY_LOW") <= st.session_state.max_ral) | 
            (col("SALARY_LOW").is_null())
        )
        df_candidates = df_candidates.sort(
            col("SALARY_LOW").is_null().asc()   # False (non-null) comes before True (null)
        )

    if st.session_state.date_available:
        df_candidates = df_candidates.filter(
            (col("DATE_AVAILABLE") <= lit(st.session_state.date_available)) |
            (col("DATE_AVAILABLE").is_null())
        )
        df_candidates = df_candidates.sort(
            col("DATE_AVAILABLE").is_null().asc(),   # non-null first
            col("DATE_AVAILABLE").asc()              # then earliest dates
        )

    if st.session_state.will_relocate:
        df_candidates = df_candidates.filter(
            (col("WILL_RELOCATE") == True) | 
            (col("WILL_RELOCATE").is_null())
        )
        df_candidates = df_candidates.sort(
            col("WILL_RELOCATE").is_null().asc(),  # non-null first
            col("WILL_RELOCATE").desc()            # True first, False second
        )

    if st.session_state.desired_locations:
        mapping = {level: i+1 for i, level in enumerate(desired_locations)}

        case_expr = "CASE "
        for k, v in mapping.items():
            case_expr += f"WHEN lower(trim(DESIRED_LOCATIONS)) = '{k.lower()}' THEN {v} "
        case_expr += "ELSE NULL END"

        df_candidates = df_candidates.with_column("loc_rank", expr(case_expr))

        threshold_pos = mapping[st.session_state.desired_locations]
        df_candidates = df_candidates.filter(col("loc_rank") >= lit(threshold_pos)) 

    if st.session_state.turno_preferenza:
        selected = st.session_state.turno_preferenza

        # Flatten the array into rows
        df_flat = df_candidates.join_table_function(
            "flatten",
            col("turno_preferenza")
        ).select(
            df_candidates["*"],
            col("value").alias("turno_value")
        )

        # Keep only rows where turno_value is one of the selected values
        df_filtered = df_flat.filter(
            col("turno_value").isin(selected)
        )

        # AND logic: candidate must match ALL selected values
        df_match_all = (
            df_filtered
            .group_by("candidateid")
            .agg(count_distinct("turno_value").alias("tv"))
            .filter(col("tv") == len(selected))
        )
        # Join back to original rows
        df_candidates = df_match_all.join(df_candidates, "candidateid")
        df_candidates = df_candidates.with_column("TURNO_PREFERENZA", to_varchar(col("TURNO_PREFERENZA")))

    if st.session_state.parttime_preferenza_perc:
        df_candidates = df_candidates.filter(
            (col("PARTTIME_PREFERENZA_PERC") >= st.session_state.parttime_preferenza_perc) | 
            (col("PARTTIME_PREFERENZA_PERC").is_null())
        )
        df_candidates = df_candidates.sort(
            col("PARTTIME_PREFERENZA_PERC").is_null().asc()   # False (non-null) comes before True (null)
        )


    # Skills filter
    if(False):
        if st.session_state.skills.strip():
            skills_list = [s.strip().lower() for s in st.session_state.skills.split(",")]
            for skill in skills_list:
                df_candidates = df_candidates.filter(lower(col("SKILLS")).like(f"%{skill}%"))

        # Skills filter
        if st.session_state.skills_certifications_mand and st.session_state.skills_certifications.strip():
            skills_list = [s.strip().lower() for s in st.session_state.skills_certifications.split(",")]
            for skill in skills_list:
                df_candidates = df_candidates.filter(lower(col("CERTIFICATIONS")).like(f"%{skill}%"))

    if st.session_state.skills_languages_mand:
        skills_list = [s.lower() for s in st.session_state.skills_languages_mand]
        for skill in skills_list:
            df_candidates = df_candidates.filter(lower(col("LANGUAGES")).like(f"%{skill}%"))

    if st.session_state.skills_education_mand:
        mapping = {level: i+1 for i, level in enumerate(education_levels)}

        case_expr = "CASE "
        for k, v in mapping.items():
            case_expr += f"WHEN lower(trim(EDUCATION)) = '{k.lower()}' THEN {v} "
        case_expr += "ELSE NULL END"

        df_candidates = df_candidates.with_column("edu_rank", expr(case_expr))

        threshold_pos = mapping[st.session_state.skills_education_mand]
        df_candidates = df_candidates.filter(col("edu_rank") >= lit(threshold_pos)) 
        
    # --- Get reference location ---
    df_location = (
        session.table("IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_ITALIAN_CITIES")
        .filter(lower(trim(col("CITY_NAME"))) == st.session_state.location.strip().lower())
    )
    location_coords = df_location.collect()

    if not location_coords:
        st.warning("Location non trovata.")
    else:
        location_dict = location_coords[0].as_dict()
        lat_ref = location_dict.get("LATITUDE")
        lon_ref = location_dict.get("LONGITUDE")

        df_candidates = df_candidates.with_column("distance_km", haversine(lat_ref, lon_ref))
        df_candidates = df_candidates.filter(col("distance_km") <= st.session_state.max_distance)

        df_candidates = df_candidates.drop("LATITUDE", "LONGITUDE")
    return df_candidates

if st.button("Cerca candidati"):
    with st.spinner("Recupero candidati e generazione ranking AI..."):

        db = llm.config.database
        schema = llm.config.schema
        table = llm.config.input_table

        columns = llm.config.columns  

        df_candidates = filter_candidates()

        # Count rows in the Snowpark DataFrame
        candidate_count = df_candidates.count()
        
        if candidate_count == 0:
            st.warning("Nessun candidato trovato")
        else:
            prompts = llm.create_prompt_row(
                st.session_state.role,
                st.session_state.skills_mand,
                st.session_state.skills_opt,
                [], #st.session_state.skills_languages_mand,
                st.session_state.skills_languages_opt,
                [], #st.session_state.skills_education_mand,
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


            top_candidates = llm.call_ai_row(session, prompts).limit(100)

            top_candidates = top_candidates.with_column("URL",concat(lit("https://cls70.bullhornstaffing.com/BullhornSTAFFING/OpenWindow.cfm?Entity=Candidate&id="),col("CANDIDATEID")))
            top_candidates = top_candidates.sort(col("SCORE").desc())

            cols_default, cols = columns_to_show()
            top_candidates = top_candidates.select(cols)

            top_candidates = top_candidates.to_pandas()
            top_candidates["AGE"] = top_candidates["AGE"].astype("Int64")
            top_candidates["URL"] = top_candidates["URL"].apply(lambda x: f"[Open Link]({x})")

            st.markdown(
                f"*Top candidates:*"
            )
            st.markdown(top_candidates.to_markdown(index=False), unsafe_allow_html=True)

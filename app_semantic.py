# Import python packages
import streamlit as st
from snowflake.core import Root
from autoMatch.utils.snowflake_utils import get_snowpark_session

from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.search_engine import SearchEngine

import os
from dotenv import load_dotenv


# Constants
ARRAY_ATTRIBUTES = {"AMENITIES"}


def get_column_specification(search_engine):
    """
    Returns the name of the search column and a list of the names of the attribute columns
    for the provided cortex search service
    """
    #session = get_active_session()
    _, attribute_columns, search_columns, columns = search_engine.get_column_specification(session)

    st.session_state.attribute_columns = attribute_columns
    st.session_state.search_column = search_columns
    st.session_state.columns = columns


def init_layout(search_engine):

    search_service, _, _, _ = search_engine.get_column_specification(session)

    st.title("Cortex AI Search")
    st.markdown(f"Querying service: `{search_service}`".replace('"', ''))


def init_search_input():
    st.session_state.query = st.text_input("Query",
                                           value="Sto cercando un Data Scientist con esperienza in Python, Kafka, AirFlow, ETL"
                                           )

def init_location_input():
    st.session_state.location = st.text_input("Province",
                                           value="Torino",
                                           )
def init_max_age_input():
    st.session_state.max_age = st.number_input(
        "Maximum age",
        min_value=18,
        #max_value=100,
        value=40,
        step=1
    )
                                   
def init_limit_input():
    st.session_state.limit = st.number_input("Limit", min_value=1, value=5)



def display_search_results(results):
    """
    Display the search results in the Streamlit UI
    """
    st.subheader("Search Results")

    # Get the column to highlight (e.g., 'description')
    search_column = st.session_state.get("search_column", "description")

    for i, result in enumerate(results):
        result = dict(result)  # Ensure it's a plain dict
        container = st.expander(f"[Result {i + 1}]", expanded=True)

        # Display other attributes
        for column, column_value in sorted(result.items()):
            if column == search_column or column.startswith("@"):
                continue  # Skip the main column and metadata
            container.markdown(f"**{column.capitalize()}**: {column_value}")



# Get the current credentials
#session = get_active_session()
@st.cache_resource
def get_snowpark_session_streamlit():
    return get_snowpark_session()

@st.cache_resource
def get_search_engine():
    config = ConfigurationManager()
    search_engine_config = config.get_search_engine_config()
    search_engine = SearchEngine(config=search_engine_config)

    return search_engine


# ✅ This will only run once per session
session = get_snowpark_session_streamlit()

search_engine = get_search_engine()


init_layout(search_engine)
get_column_specification(search_engine)

init_location_input()
init_max_age_input()
init_limit_input()
init_search_input()



if st.session_state.query:
    
    results = search_engine.query_cortex_search_service(session, 
                                                        query=st.session_state.query, 
                                                        filter=search_engine.create_filter(st.session_state.max_age, st.session_state.location), 
                                                        limit=st.session_state.limit)
    
    display_search_results(results)



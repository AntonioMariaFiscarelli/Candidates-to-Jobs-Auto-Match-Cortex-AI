# Import python packages
import streamlit as st
from snowflake.core import Root
from autoMatch.utils.snowflake_utils import get_snowpark_session

from autoMatch.config.configuration import ConfigurationManager
from autoMatch.components.search_engine import SearchEngine

import os
from dotenv import load_dotenv


# Constants
DB = "cortex_search_tutorial_db"
SCHEMA = "public"
SERVICE = "airbnb_svc"
BASE_TABLE = "cortex_search_tutorial_db.public.airbnb_listings"
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

'''
def query_cortex_search_service(query, filter={}):
    """
    Queries the cortex search service in the session state and returns a list of results
    """
    session = get_active_session()
    cortex_search_service = (
        Root(session)
        .databases[DB]
        .schemas[SCHEMA]
        .cortex_search_services[SERVICE]
    )
    context_documents = cortex_search_service.search(
        query,
        columns=st.session_state.columns,
        filter=filter,
        limit=st.session_state.limit)
    return context_documents.results
'''

"""
@st.cache_data
def distinct_values_for_attribute(col_name, is_array_attribute=False):
    session = get_active_session()
    if is_array_attribute:
        values = session.sql(f'''
        SELECT DISTINCT value FROM {BASE_TABLE},
        LATERAL FLATTEN(input => {col_name})
        ''').collect()
    else:
        values = session.sql(f"SELECT DISTINCT {col_name} AS VALUE FROM {BASE_TABLE}").collect()
    return [ x["VALUE"].replace('"', "") for x in values ]
""" 

def init_search_input():
    st.session_state.query = st.text_input("Query",
                                           placeholder="I'm looking for a Data Scientist with Python and SQL skills"
                                           )

def init_limit_input():
    st.session_state.limit = st.number_input("Limit", min_value=1, value=5)


'''
def init_attribute_selection():
    st.session_state.attributes = {}
    for col in st.session_state.attribute_columns:
        is_multiselect = col in ARRAY_ATTRIBUTES
        st.session_state.attributes[col] = st.multiselect(
            col,
            distinct_values_for_attribute(col, is_array_attribute=is_multiselect)
        )
'''


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

        # Safely get the main description or fallback
        #description_text = result.get(search_column, "No description available")
        #container.markdown(f"**{search_column.capitalize()}**: {description_text}")

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


# ✅ This will only run once per session
session = get_snowpark_session_streamlit()



config = ConfigurationManager()
search_engine_config = config.get_search_engine_config()
search_engine = SearchEngine(config=search_engine_config)

init_layout(search_engine)
get_column_specification(search_engine)
#init_attribute_selection()
init_limit_input()
init_search_input()

#results = query_cortex_search_service(
#    st.session_state.query,
#    filter = create_filter_object(st.session_state.attributes)
#)

if st.session_state.query:
    
    results = search_engine.query_cortex_search_service(session, 
                                                        query=st.session_state.query, 
                                                        filter=search_engine.create_filter(40), 
                                                        limit=5)
        
    display_search_results(results)



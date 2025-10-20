# 🧠 Candidates-to-Jobs Auto-Match with Snowflake Cortex AI

This project builds a semantic search engine that automatically matches job candidates to relevant job postings using Snowflake Cortex AI. It combines natural language search, structured filtering, and Snowpark to deliver intelligent, scalable matching logic for recruiting and talent platforms.

## 🚀 Features

- **Cortex Search Service**: Uses Snowflake’s semantic indexing to match candidates to jobs based on resume content and job descriptions.
- **Structured Filtering**: Supports filters like location, age, and skills.
- **Streamlit UI**: Interactive front-end for entering queries, selecting filters, and viewing ranked results.
- **Snowpark Integration**: Executes SQL and Python logic inside Snowflake for efficient data handling.
- **Dynamic Filter Builder**: Constructs Cortex-compatible filter objects.


## 🛠️ Setup Instructions

1. **Clone the repository**

```bash
git clone https://github.com/AntonioMariaFiscarelli/Candidates-to-Jobs-Auto-Match-Cortex-AI
cd Candidates-to-Jobs-Auto-Match-Cortex-AI
```

2. **Install dependencies**
pip install -r requirements.txt

3. **Configure environment**
Create a .env file with your Snowflake credentials and warehouse name:
SNOWFLAKE_WAREHOUSE=your_warehouse_name

3. **Run the Stramlit app**
streamlit run app.py

🧠 How It Works
The Cortex Search Service indexes semantic columns such as last_job, skills, and experience. Structured fields like location and age are used as attributes for filtering. Users can enter a natural language query (e.g., "Senior data scientist with Python and NLP experience") and apply filters. The app returns top-ranked candidates based on semantic similarity and filter criteria.

📊 Example Use Case
A recruiter enters:

“Senior data scientist with Python and NLP experience”

The app:
- Searches semantically across candidate job history and skills
- Filters by location and age if selected
- Returns top-ranked candidates with matching profiles

📄 License
This project is licensed under the MIT License.

🤝 Contributing
Feel free to fork the repository, submit issues, or open pull requests to improve functionality or add new features.
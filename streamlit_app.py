import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import google.generativeai as genai
import bcrypt
from utils import get_db_url  # should return your Postgres connection string

# Load environment variables
load_dotenv()

# Secrets
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

# Database schema for AI context
DATABASE_SCHEMA = """
Database Schema:

LOOKUP TABLES:
- ProductCategory(ProductCategoryID, ProductCategory, ProductCategoryDescription)

CORE TABLES:
- Region(RegionID, Region)
- Country(CountryID, Country, RegionID)
- Customer(CustomerID, FirstName, LastName, Address, City, CountryID)
- Product(ProductID, ProductName, ProductUnitPrice, ProductCategoryID)
- OrderDetail(OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered)

IMPORTANT NOTES:
- Use JOINs to get descriptive values (e.g., join Product to ProductCategory)
- OrderDate is DATE type
- QuantityOrdered is INTEGER
- For aggregates, use SUM, AVG, COUNT as needed
- Add LIMIT clauses for queries returning many rows
"""

# ----------------------
# Login functionality
# ----------------------
def login_screen():
    st.title("🔐 Secure Login")
    st.markdown("---")
    st.write("Enter your password to access the AI SQL Query Assistant.")
    
    password = st.text_input("Password", type="password", key="login_password")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("🔓 Login", type="primary", use_container_width=True)
    
    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), HASHED_PASSWORD):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")
    
    st.markdown("---")
    st.info("""
    **Security Notice:**
    - Passwords are protected using bcrypt hashing
    - Your session is secure and isolated
    - You will remain logged in until you close the browser or click logout
    """)

def require_login():
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()

# ----------------------
# Database connection
# ----------------------
@st.cache_resource
def get_db_connection():
    try:
        DATABASE_URL = get_db_url()
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def run_query(sql):
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# ----------------------
# Gemini AI Client
# ----------------------
@st.cache_resource
def get_openai_client():
    genai.configure(api_key=OPENAI_API_KEY)
    return genai.GenerativeModel("models/gemini-2.5-flash")

def extract_sql_from_response(response_text):
    clean_sql = re.sub(r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE).strip()
    return clean_sql

def generate_sql_with_gpt(user_question):
    model = get_openai_client()
    prompt = f"""You are a PostgreSQL expert. Given the following database schema and a user's question, generate a valid PostgreSQL query.

{DATABASE_SCHEMA}

User Question: {user_question}

Requirements:
1. Generate ONLY the SQL query.
2. Use proper JOINs to get descriptive names.
3. Use SUM, AVG, COUNT when appropriate.
4. Use LIMIT 100 for queries returning many rows.
5. Format date columns correctly.
6. Add helpful column aliases using AS.

Generate the SQL query:"""

    try:
        response = model.generate_content(prompt)
        sql_query = extract_sql_from_response(response.text)
        return sql_query
    except Exception as e:
        st.error(f"Error calling Gemini API: {e}")
        return None

# ----------------------
# Streamlit App
# ----------------------
def main():
    require_login()
    st.title("🤖 AI-Powered SQL Query Assistant")
    st.markdown("Ask questions in natural language, and I will generate SQL queries for you to review and run!")
    st.markdown("---")

    st.sidebar.title("💡 Example Questions")
    st.sidebar.markdown("""
Try asking questions like:

**Demographics:**
- How many customers do we have by city?
- How many customers do we have by country?

**Products & Sales:**
- What are the top 10 products by quantity sold?
- What is the total sales by product category?
- How many orders per customer?

**Orders:**
- Average quantity ordered per order
- Total sales per month
""")
    st.sidebar.markdown("---")
    st.sidebar.info("""
        🩼**How it works:**
        1. Enter your question in plain English
        2. AI generates SQL query
        3. Review and optionally edit the query
        4. Click "Run Query" to execute           
    """)

    if st.sidebar.button("🚪Logout"):
        st.session_state.logged_in = False
        st.rerun()

    # ----------------------
    # Session state
    # ----------------------
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None

    # ----------------------
    # User input
    # ----------------------
    user_question = st.text_area(
        " What would you like to know?",
        height=100,
        placeholder="What is the total sales by product category?"
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        generate_button = st.button(" Generate SQL", type="primary", use_container_width=True)
    with col2:
        if st.button(" Clear History", use_container_width=True):
            st.session_state.query_history = []
            st.session_state.generated_sql = None
            st.session_state.current_question = None

    # ----------------------
    # Generate SQL
    # ----------------------
    if generate_button and user_question:
        user_question = user_question.strip()
        if st.session_state.current_question != user_question:
            st.session_state.generated_sql = None
            st.session_state.current_question = None

        with st.spinner("🧠 AI is thinking and generating SQL..."):
            sql_query = generate_sql_with_gpt(user_question)
            if sql_query:
                st.session_state.generated_sql = sql_query
                st.session_state.current_question = user_question

    # ----------------------
    # Show & Run SQL
    # ----------------------
    if st.session_state.generated_sql:
        st.markdown("---")
        st.subheader("Generated SQL Query")
        st.info(f"**Question:** {st.session_state.current_question}")

        edited_sql = st.text_area(
            "Review and edit the SQL query if needed:",
            value=st.session_state.generated_sql,
            height=200
        )

        if st.button("Run Query", type="primary", use_container_width=True):
            with st.spinner("Executing query ..."):
                df = run_query(edited_sql)
                if df is not None:
                    st.session_state.query_history.append(
                        {'question': user_question, 'sql': edited_sql, 'rows': len(df)}
                    )
                    st.markdown("---")
                    st.subheader("📊 Query Results")
                    st.success(f"✅ Query returned {len(df)} rows")
                    st.dataframe(df, width="stretch")

    # ----------------------
    # Query history
    # ----------------------
    if st.session_state.query_history:
        st.markdown('---')
        st.subheader("📜 Query History")
        for idx, item in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history)-idx}: {item['question'][:60]}..."):
                st.markdown(f"**Question:** {item['question']}")
                st.code(item["sql"], language="sql")
                st.caption(f"Returned {item['rows']} rows")
                if st.button(f"Re-run this query", key=f"rerun_{idx}"):
                    df = run_query(item["sql"])
                    if df is not None:
                        st.dataframe(df, width="stretch")

if __name__ == "__main__":
    main()

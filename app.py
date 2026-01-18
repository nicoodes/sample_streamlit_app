import os
import sys
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px

# Make `src` importable so we can import `tenis_api` without requiring it to be a package
# This works when running `streamlit run app.py` from the project root.
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
st.set_page_config(page_title="Sample app api", layout="wide")

try:
    from tenis_api import get_fixtures
except Exception:
    # If import fails, set placeholder to None and show an error later when fetching
    get_fixtures = None
def get_secret(key: str, env_var: str | None = None):
    """Return secret from Streamlit secrets if available, otherwise from env.

    This wraps access to `st.secrets` because referencing `st.secrets`
    when no secrets.toml exists can raise a StreamlitSecretNotFoundError.
    """
    try:
        # Try Streamlit secrets first (may raise if no secrets file present)
        val = None
        if hasattr(st, "secrets"):
            # st.secrets behaves like a dict when present
            val = st.secrets.get(key)
    except Exception:
        val = None

    if val is None:
        return os.getenv(env_var or key)
    return val


API_KEY = get_secret("API_KEY", env_var="API_KEY")
PASSWORD_SECRET = get_secret("PASSWORD", env_var="APP_PASSWORD")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

st.title("Sample Streamlit App")

if not st.session_state.logged_in:
    if not PASSWORD_SECRET:
        st.info("No password set. Add `PASSWORD` in Streamlit secrets or `APP_PASSWORD` in .env for local testing.")
    pwd = st.text_input("Enter password", type="password", key="pwd")
    # Local flag to render the logged-in UI immediately after a successful login
    show_logged_in = False
    if st.button("Login"):
        if PASSWORD_SECRET and pwd == PASSWORD_SECRET:
            st.session_state.logged_in = True
            show_logged_in = True
        else:
            st.error("Invalid password")

# Render logged-in UI when session state indicates logged in or just logged in
if st.session_state.logged_in or ("show_logged_in" in locals() and show_logged_in):
    st.success("Logged in")
    st.write("Welcome — this is a minimal sample app.")
    st.markdown("---")
    # --- Fixtures UI ---
    st.header("Fetch Tennis Fixtures")

    # Date selectors using Streamlit's date_input (converts to string for the API)
    default_start = datetime.now().date()
    default_stop = datetime.now().date()

    date_start = st.date_input("Start date", value=default_start)
    date_stop = st.date_input("Stop date", value=default_stop)

    # Convert date objects to YYYY-MM-DD strings for the API
    date_start_str = date_start.strftime("%Y-%m-%d")
    date_stop_str = date_stop.strftime("%Y-%m-%d")

    # Player key input: default empty string (numbers entered will be kept as strings)
    player_key = st.text_input("Player key", value="")

    # Fetch button and results
    if st.button("Fetch fixtures"):
        if get_fixtures is None:
            st.error("Could not import `get_fixtures` from `src/tenis_api.py`. Ensure the file exists.")
        else:
            with st.spinner("Fetching fixtures..."):
                df = get_fixtures(date_start=date_start_str, date_stop=date_stop_str, player_key=player_key)
            if df is None:
                st.info("No data returned.")
            else:
                st.session_state.fixtures_df = df

    # Show table if available in session state
    if "fixtures_df" in st.session_state:
        st.subheader("Fixtures")
        st.dataframe(st.session_state.fixtures_df)
        csv = st.session_state.fixtures_df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", data=csv, file_name="fixtures.csv", mime="text/csv")
        
        # Plot events by type
        if "event_type_type" in st.session_state.fixtures_df.columns:
            counts = (
                st.session_state.fixtures_df["event_type_type"]
                .fillna("Unknown")
                .value_counts()
                .reset_index(name="count")
                .rename(columns={"index": "event_type_type"})
            )
            fig = px.bar(
                counts,
                x="event_type_type",
                y="count",
                color="event_type_type",
                labels={"event_type_type": "Event type", "count": "Event count"},
                title=f"Events by type for player {player_key} and date {date_start_str} to {date_stop_str}" if player_key else f"Events by type for date {date_start_str} to {date_stop_str}",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No `event_type_type` column available to plot.")


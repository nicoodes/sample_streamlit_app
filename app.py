import os
import streamlit as st
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
    name = st.text_input("Type your name")
    if name:
        st.write(f"Hello, {name}!")

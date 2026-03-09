import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "startup_events.db"

st.set_page_config(page_title="Startup Idea Events Dashboard", layout="wide")

st.title("Dashboard za obradu startup događaja")
st.write("Pregled obrađenih događaja i osnovne statistike validacije startup ideja.")


def load_events():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT event_id, event_type, business_id, timestamp, payload_json
        FROM events
        ORDER BY timestamp DESC
    """, conn)
    conn.close()
    return df


def load_stats():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT business_id,
               submitted_count,
               validation_requested_count,
               validation_completed_count,
               validation_failed_count,
               feedback_viewed_count,
               last_event_timestamp
        FROM idea_stats
        ORDER BY last_event_timestamp DESC
    """, conn)
    conn.close()
    return df


events_df = load_events()
stats_df = load_stats()

st.subheader("Osnovni KPI pokazatelji")

col1, col2, col3 = st.columns(3)
col1.metric("Ukupno događaja", len(events_df))
col2.metric("Ukupno startup ideja", len(stats_df))
col3.metric(
    "Uspešne validacije",
    int(stats_df["validation_completed_count"].sum()) if not stats_df.empty else 0
)

st.subheader("Statistika po startup ideji")
st.dataframe(stats_df, use_container_width=True)

st.subheader("Pregled svih događaja")
st.dataframe(events_df, use_container_width=True)

if not stats_df.empty:
    st.subheader("Broj uspešnih validacija po ideji")
    chart_data = stats_df[["business_id", "validation_completed_count"]].set_index("business_id")
    st.bar_chart(chart_data)
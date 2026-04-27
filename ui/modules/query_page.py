"""Query module page."""

import streamlit as st

from common import api_post, format_response


def render(base_url: str) -> None:
    st.title("🔍 Query")
    st.markdown("Build query patterns for statistics, detection, or exploration on indexed logs.")

    method = st.selectbox("Query method", ["statistics", "detection", "exploration"])

    with st.form("query_form"):
        log_name = st.text_input("Log name", "example_log")
        storage_namespace = st.text_input("Storage namespace", "siesta")
        pattern = st.text_input("Query pattern", "A B")
        support_threshold = st.number_input(
            "Support threshold",
            value=0.0,
            min_value=0.0,
            max_value=1.0,
            format="%.2f",
        )
        explore_mode = "accurate"
        explore_k = 10

        if method == "exploration":
            explore_mode = st.selectbox("Explore mode", ["accurate", "fast", "hybrid"], index=0)
            explore_k = st.number_input("Explore K", value=10, min_value=0)

        submit = st.form_submit_button("Run query")

    if submit:
        query_config = {
            "log_name": log_name,
            "storage_namespace": storage_namespace,
            "query": {
                "pattern": pattern,
                "explore_mode": explore_mode,
                "explore_k": explore_k,
            },
            "support_threshold": support_threshold,
        }
        response = api_post(f"executor/{method}", base_url, payload=query_config)
        format_response(response)

    with st.expander("Need help?"):
        st.markdown(
            "- `statistics` and `detection` use the current query pattern directly.\n"
            "- `exploration` uses the pattern as a seed and can return similar matches.\n"
            "- Keep support thresholds low for exploratory analysis and higher for precise detection."
        )

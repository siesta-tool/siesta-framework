"""Miner module page."""

import streamlit as st

from common import api_post, format_response


def render(base_url: str) -> None:
    st.title("⛏️ Miner")
    st.markdown("Mine declarative constraints from an indexed log and inspect the configuration before sending it.")

    with st.form("miner_form"):
        col1, col2 = st.columns(2)
        with col1:
            log_name = st.text_input("Log name", "example_log")
            storage_namespace = st.text_input("Storage namespace", "siesta")
            categories = st.multiselect(
                "Categories",
                ["*", "positional", "existential", "ordered", "unordered", "negation"],
                default=["*"],
            )
            grouping = st.selectbox("Grouping", ["trace", "window"], index=0)

        with col2:
            window_size = st.number_input("Window size", value=30, min_value=1)
            support_threshold = st.number_input(
                "Support threshold",
                value=0.0,
                min_value=0.0,
                max_value=1.0,
                format="%.2f",
            )
            include_trace_lists = st.checkbox("Include trace lists", value=False)
            force_recompute = st.checkbox("Force recompute", value=False)

        submit = st.form_submit_button("Run miner")

    if submit:
        mining_config = {
            "log_name": log_name,
            "storage_namespace": storage_namespace,
            "categories": categories,
            "grouping": grouping,
            "window_size": window_size,
            "support_threshold": support_threshold,
            "include_trace_lists": include_trace_lists,
            "force_recompute": force_recompute,
        }
        response = api_post("miner/run", base_url, payload=mining_config)
        format_response(response)

    with st.expander("Need help?"):
        st.markdown(
            "- Use `categories` to narrow the mining output by constraint type.\n"
            "- `Force recompute` overrides any cached results on the backend.\n"
            "- `Include trace lists` can produce more detailed results at the cost of larger responses."
        )

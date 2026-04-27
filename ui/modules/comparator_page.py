"""Comparator module page."""

import streamlit as st

from common import api_post, format_response, parse_group_definitions


def render(base_url: str) -> None:
    st.title("⚖️ Comparator")
    st.markdown("Compare log fragments using n-grams, rare rules, or targeted rules with a flexible separator definition.")

    method = st.selectbox("Comparison method", ["ngrams", "rare_rules", "targeted_rules"])

    if "comparator_running" not in st.session_state:
        st.session_state.comparator_running = False
    if "comparator_submit_requested" not in st.session_state:
        st.session_state.comparator_submit_requested = False

    with st.form("comparator_form"):
        log_name = st.text_input("Log name", "example_log")
        storage_namespace = st.text_input("Storage namespace", "siesta")
        separating_key = st.text_input("Separating key", "activity")
        separating_groups_text = st.text_area(
            "Separating groups (one comma-separated group per line)",
            value="",
            height=120,
        )
        support_threshold = st.number_input(
            "Support threshold",
            value=0.0,
            min_value=0.0,
            max_value=1.0,
            format="%.2f",
        )
        n = st.number_input("N-gram length", value=2, min_value=1)
        vis = False
        target_label = None
        filtering_support = None

        if method == "ngrams":
            vis = st.checkbox("Generate visualization", value=False)
        elif method == "targeted_rules":
            target_label = st.number_input("Target label", value=1, min_value=0)
            filtering_support = st.number_input(
                "Filtering support",
                value=1.0,
                min_value=0.0,
                max_value=1.0,
                format="%.2f",
            )

        submit = st.form_submit_button("Run comparator", disabled=st.session_state.comparator_running, key="run_comparator_button")
        if submit:
            st.session_state.comparator_submit_requested = True

    if st.session_state.comparator_submit_requested and not st.session_state.comparator_running:
        st.session_state.comparator_running = True
        st.session_state.comparator_submit_requested = False
        with st.spinner("Running comparator..."):
            comparator_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "method_params": {"n": n},
                "separating_key": separating_key,
                "separating_groups": parse_group_definitions(separating_groups_text),
                "support_threshold": support_threshold,
            }
            if method == "ngrams" and vis:
                comparator_config["method_params"]["vis"] = True
            if method == "targeted_rules":
                comparator_config["method_params"]["target_label"] = target_label
                comparator_config["method_params"]["filtering_support"] = filtering_support

            response = api_post(f"comparing/{method}", base_url, payload=comparator_config)
            format_response(response)
        st.session_state.comparator_running = False

    with st.expander("Need help?"):
        st.markdown(
            "- Define each separating group on its own line using comma-separated values.\n"
            "- `Generate visualization` is available only for n-gram comparisons.\n"
            "- Use `target_label` for targeted rule discovery."
        )

"""Analyser module page."""

import streamlit as st

from common import api_post, format_response, parse_comma_list, parse_optional, parse_optional_float, parse_optional_int


def render(base_url: str) -> None:
    st.title("🧠 Analyser")
    st.markdown("Run analysis methods on indexed logs, such as directly-follow, durations, loop detection, and attribute deviations.")

    method = st.selectbox(
        "Analyser method",
        ["directly_follows", "durations", "loop_detection", "attribute_deviations"],
    )

    with st.form("analyser_form"):
        log_name = st.text_input("Log name", "example_log")
        storage_namespace = st.text_input("Storage namespace", "siesta")

        if method == "directly_follows":
            end_time = parse_optional(st.text_input("End time attribute", ""))
            support_threshold = parse_optional_float(st.text_input("Support threshold", ""))
            filter_out = st.checkbox("Filter out rare items", value=False)
            include_traces = st.checkbox("Include trace IDs", value=False)
            return_csv = st.checkbox("Return CSV", value=False)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "end_time": end_time,
                "support_threshold": support_threshold,
                "filter_out": filter_out,
                "include_traces": include_traces,
                "return_csv": return_csv,
            }

        elif method == "durations":
            duration_mode = st.selectbox("Duration mode", ["activity", "group"], index=0)
            end_time = parse_optional(st.text_input("End time attribute", ""))
            grouping_key = parse_optional(st.text_input("Grouping key", ""))
            grouping_value = parse_optional(st.text_input("Grouping value", ""))
            per_group = st.checkbox("Per group output", value=False)
            return_csv = st.checkbox("Return CSV", value=False)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "duration_mode": duration_mode,
                "end_time": end_time,
                "grouping_key": grouping_key,
                "grouping_value": grouping_value,
                "per_group": per_group,
                "return_csv": return_csv,
            }

        elif method == "loop_detection":
            grouping_key = parse_optional(st.text_input("Grouping key", ""))
            grouping_value = parse_optional(st.text_input("Grouping value", ""))
            min_timestamp = parse_optional_int(st.text_input("Minimum timestamp (epoch seconds)", ""))
            support_threshold = parse_optional_float(st.text_input("Support threshold", ""))
            filter_out = st.checkbox("Filter out rare loops", value=False)
            top_k = parse_optional_int(st.text_input("Top K loops", ""))
            trace_based = st.checkbox("Include trace IDs", value=False)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "grouping_key": grouping_key,
                "grouping_value": grouping_value,
                "min_timestamp": min_timestamp,
                "support_threshold": support_threshold,
                "filter_out": filter_out,
                "top_k": top_k,
                "trace_based": trace_based,
            }

        else:
            steps = st.multiselect(
                "Steps",
                [0, 1, 2, 3, 4],
                default=[0, 1, 2, 3, 4],
            )
            excluded_attributes = parse_comma_list(st.text_input("Excluded attributes", ""))
            surprise_threshold = st.number_input("Surprise threshold", value=4.0, format="%.2f")
            zscore_threshold = st.number_input("Z-score threshold", value=3.5, format="%.2f")
            ngram_n = st.number_input("N-gram length", value=2, min_value=1)
            min_group_size = st.number_input("Minimum group size", value=5, min_value=1)
            n_buckets = st.number_input("N buckets", value=5, min_value=1)
            support_threshold = parse_optional_float(st.text_input("Support threshold", ""))
            filter_out = st.checkbox("Filter out by support", value=False)
            on_rare = parse_optional_float(st.text_input("Rare-mode support threshold", ""))
            output_format = st.selectbox("Output format", ["json", "csv", "html"], index=0)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "steps": steps,
                "excluded_attributes": excluded_attributes,
                "surprise_threshold": surprise_threshold,
                "zscore_threshold": zscore_threshold,
                "ngram_n": ngram_n,
                "min_group_size": min_group_size,
                "n_buckets": n_buckets,
                "support_threshold": support_threshold,
                "filter_out": filter_out,
                "on_rare": on_rare,
                "output_format": output_format,
            }

        submit = st.form_submit_button("Run analyser")

    if submit:
        response = api_post(f"analyser/{method}", base_url, payload=analyser_config)
        format_response(response)

    with st.expander("Need help?"):
        st.markdown(
            "- Choose the analyser method to update the form fields automatically.\n"
            "- `Return CSV` returns CSV output when the endpoint supports it.\n"
            "- Use empty values for optional settings to keep the default backend behavior."
        )

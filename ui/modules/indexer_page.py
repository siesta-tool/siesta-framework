"""Indexer module page."""

import json

import streamlit as st

from common import api_post, format_response


def parse_attribute_list(raw: str) -> list[str] | None:
    if not raw.strip():
        return None
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_field_value(raw: str) -> str | list[str] | None:
    """Parse a field-mapping text input into a single column name or a list of column names.

    A comma-separated value maps the field to the combination of those columns (joined with
    the configured composite separator at indexing time), matching parse_attribute_list's
    comma convention. Blank means unmapped/computed.
    """
    parts = [item.strip() for item in raw.split(",") if item.strip()]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else parts


def _display_response(response) -> None:
    if isinstance(response, dict) and response.get("error"):
        st.error(response["error"])
        return
    if isinstance(response, dict) and response.get("status_code", 0) >= 400:
        st.error(f"HTTP {response['status_code']}")
        st.code(response.get("text", ""))
        return

    elapsed = response.get("time") if isinstance(response, dict) else None
    if elapsed is not None:
        st.success(f"Indexing completed in {float(elapsed):.2f} s")
        rest = {k: v for k, v in response.items() if k != "time"}
        if rest:
            st.json(rest)
    else:
        format_response(response)


def render(base_url: str) -> None:
    if "indexer_running" not in st.session_state:
        st.session_state.indexer_running = False
    if "indexer_submit_requested" not in st.session_state:
        st.session_state.indexer_submit_requested = False

    st.title("🧩 Indexer")
    st.markdown(
        "Upload logs and build index configurations for batch or streaming ingestion. "
        "Use the form below to customize the log name, storage namespace, and field mappings."
    )

    default_field_mappings = {
        "xes": {
            "activity": "concept:name",
            "trace_id": "concept:name",
            "position": None,
            "start_timestamp": "time:timestamp",
            "attributes": ["*"],
        },
        "csv": {
            "activity": "activity",
            "trace_id": "trace_id",
            "position": "position",
            "start_timestamp": "timestamp",
            "attributes": ["resource", "cost"],
        },
        "json": {
            "activity": "activity",
            "trace_id": "caseID",
            "position": "position",
            "start_timestamp": "Timestamp",
            "attributes": None,
        },
    }

    enable_streaming = st.checkbox(
        "Enable streaming",
        value=False,
        help="Outside the form so the Kafka topic field below can react immediately.",
    )

    with st.form("indexer_form"):
        col1, col2 = st.columns(2)
        with col1:
            log_name = st.text_input("Log name", "example_log")
            storage_namespace = st.text_input("Storage namespace", "siesta")
            clear_existing = st.checkbox("Clear existing index", value=False)
            kafka_topic = st.text_input(
                "Kafka topic",
                value="example_log" if enable_streaming else "",
                disabled=not enable_streaming,
                help="Enable streaming to configure the Kafka topic.",
                key="kafka_topic",
            )
            if not enable_streaming:
                kafka_topic = ""
            lookback = st.text_input("Lookback window", "7d")

        with col2:
            trace_level_fields = st.text_input("Trace level fields", "trace_id")
            timestamp_fields = st.text_input("Timestamp fields", "start_timestamp")
            composite_separator = st.text_input(
                "Composite separator",
                "::",
                help="Used to join multiple columns when a field below is given as a "
                     "comma-separated combination (e.g. a trace_id field).",
            )
            with st.expander("Field mappings", expanded=False):
                st.write("Configure the mapping fields for each supported log format.")
                st.markdown("**XES mapping**")
                xes_activity = st.text_input("XES activity field", default_field_mappings["xes"]["activity"])
                xes_trace_id = st.text_input(
                    "XES trace_id field",
                    default_field_mappings["xes"]["trace_id"],
                    help="Comma-separated to combine several columns into the trace id.",
                )
                xes_position = st.text_input(
                    "XES position field",
                    default_field_mappings["xes"]["position"] or "",
                    help="Leave blank if position should be auto-inferred.",
                )
                xes_start_timestamp = st.text_input("XES timestamp field", default_field_mappings["xes"]["start_timestamp"])
                xes_attributes = st.text_input(
                    "XES attributes",
                    ", ".join(default_field_mappings["xes"]["attributes"]),
                    help="Comma-separated attribute columns, or * for all.",
                )

                st.markdown("**CSV mapping**")
                csv_activity = st.text_input("CSV activity field", default_field_mappings["csv"]["activity"])
                csv_trace_id = st.text_input(
                    "CSV trace_id field",
                    default_field_mappings["csv"]["trace_id"],
                    help="Comma-separated to combine several columns into the trace id.",
                )
                csv_position = st.text_input(
                    "CSV position field",
                    default_field_mappings["csv"]["position"],
                    help="Optional event position field.",
                )
                csv_start_timestamp = st.text_input("CSV timestamp field", default_field_mappings["csv"]["start_timestamp"])
                csv_attributes = st.text_input(
                    "CSV attributes",
                    ", ".join(default_field_mappings["csv"]["attributes"]),
                    help="Comma-separated attribute columns, or leave blank for none.",
                )

                st.markdown("**JSON mapping**")
                json_activity = st.text_input("JSON activity field", default_field_mappings["json"]["activity"])
                json_trace_id = st.text_input(
                    "JSON trace_id field",
                    default_field_mappings["json"]["trace_id"],
                    help="Comma-separated to combine several columns into the trace id.",
                )
                json_position = st.text_input(
                    "JSON position field",
                    default_field_mappings["json"]["position"],
                    help="Optional event position field.",
                )
                json_start_timestamp = st.text_input("JSON timestamp field", default_field_mappings["json"]["start_timestamp"])
                json_attributes = st.text_input(
                    "JSON attributes",
                    "",
                    help="Comma-separated attribute keys, or leave blank if none.",
                )

            
            log_file = st.file_uploader("Upload log file (XES, CSV, JSON)", type=["xes", "csv", "json"])

        submit = st.form_submit_button(
            "Run indexer",
            disabled=st.session_state.indexer_running,
        )
        if submit:
            st.session_state.indexer_submit_requested = True

    if st.session_state.indexer_submit_requested and not st.session_state.indexer_running:
        st.session_state.indexer_running = True
        st.session_state.indexer_submit_requested = False

        field_mappings = {
            "xes": {
                "activity": parse_field_value(xes_activity),
                "trace_id": parse_field_value(xes_trace_id),
                "position": xes_position or None,
                "start_timestamp": parse_field_value(xes_start_timestamp),
                "attributes": parse_attribute_list(xes_attributes),
            },
            "csv": {
                "activity": parse_field_value(csv_activity),
                "trace_id": parse_field_value(csv_trace_id),
                "position": csv_position or None,
                "start_timestamp": parse_field_value(csv_start_timestamp),
                "attributes": parse_attribute_list(csv_attributes),
            },
            "json": {
                "activity": parse_field_value(json_activity),
                "trace_id": parse_field_value(json_trace_id),
                "position": json_position or None,
                "start_timestamp": parse_field_value(json_start_timestamp),
                "attributes": parse_attribute_list(json_attributes),
            },
        }
        index_config = {
            "log_name": log_name,
            "storage_namespace": storage_namespace,
            "clear_existing": clear_existing,
            "enable_streaming": enable_streaming,
            "kafka_topic": kafka_topic,
            "lookback": lookback,
            "field_mappings": field_mappings,
            "trace_level_fields": [item.strip() for item in trace_level_fields.split(",") if item.strip()],
            "timestamp_fields": [item.strip() for item in timestamp_fields.split(",") if item.strip()],
            "composite_separator": composite_separator or "::",
            "output_path": f"output/{log_name}",
        }
        files = None
        if log_file is not None:
            files = {"log_file": (log_file.name, log_file.getvalue())}

        with st.spinner("Running indexer..."):
            response = api_post("indexing/run", base_url, payload={"index_config": json.dumps(index_config)}, files=files)

        st.session_state.indexer_running = False
        _display_response(response)

    with st.expander("Need help?"):
        st.markdown(
            "- Use `Enable streaming` when ingesting events from Kafka.\n"
            "- `Clear existing index` deletes the old index before writing new data.\n"
            "- `Field mappings` are now configured as individual form fields for each supported format."
        )

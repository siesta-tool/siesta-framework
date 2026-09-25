"""Analyser module page."""

from typing import Any

import streamlit as st
from streamlit.components.v1 import html as components_html

from common import (
    api_post, api_post_binary, format_response, log_options, namespace_options, parse_comma_list,
    parse_group_definitions, parse_optional, parse_optional_float, parse_optional_int,
)

CATEGORIES: dict[str, list[str]] = {
    "Core Analysis": ["directly_follows", "durations", "loop_detection", "attribute_deviations"],
    "Comparison": ["ngrams", "rare_rules", "targeted_rules"],
    "Process Model": ["dfg_model", "bpmn", "petri_net"],
    "Time Analytics": ["bottlenecks", "temporal_deviations"],
}
PROCESS_MODEL_METHODS = set(CATEGORIES["Process Model"])


def _maybe_pie_chart(data: dict[str, float], title: str) -> None:
    st.markdown(f"### {title}")
    try:
        import altair as alt
        import pandas as pd
    except ImportError:
        st.bar_chart(data)
        return

    values = [{"label": label, "value": float(value)} for label, value in data.items() if value is not None]
    if not values:
        return

    df = pd.DataFrame(values)
    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta(field="value", type="quantitative"),
            color=alt.Color(field="label", type="nominal"),
            tooltip=["label", "value"],
        )
        .properties(title=title)
    )
    st.altair_chart(chart, width="content")


def _build_loops_html(response: dict) -> str | None:
    try:
        from pyvis.network import Network
    except ImportError:
        return None

    net = Network(height="650px", width="100%", directed=True)
    net.toggle_physics(True)
    net.barnes_hut()

    seen_nodes: set[str] = set()

    for loop_type, entries, color in [
        ("self", response.get("self_loops", []), "blue"),
        ("non-self", response.get("non_self_loops", []), "orange"),
    ]:
        for item in entries:
            pattern = str(item.get("pattern", "")).strip()
            support = item.get("support", 0)
            nodes = [node.strip() for node in pattern.split("->") if node.strip()]
            for node in nodes:
                if node not in seen_nodes:
                    net.add_node(node, label=node, title=f"Activity {node}", color="lightgray")
                    seen_nodes.add(node)

            if len(nodes) == 1:
                net.add_edge(nodes[0], nodes[0], label=f"{support:.2f}", title=f"{pattern} ({support})", color=color)
            else:
                for i in range(len(nodes) - 1):
                    net.add_edge(
                        nodes[i],
                        nodes[i + 1],
                        label=f"{support:.2f}",
                        title=f"{pattern} ({support})",
                        color=color,
                    )

    return net.generate_html()


def _render_analyser_response(response: Any, method: str, binary_result: bytes | None = None) -> None:
    if method in PROCESS_MODEL_METHODS:
        _render_process_model_response(response, binary_result)
        return

    if isinstance(response, dict) and (response.get("error") or (response.get("status_code") and response["status_code"] >= 400)):
        format_response(response)
        return

    if isinstance(response, dict) and response.get("code") is not None:
        code = response["code"]
        message = response.get("message")
        if code != 200:
            st.error(f"Error (code {code})" + (f": {message}" if message else ""))
            return
        st.success(f"Success ({code})")

    if method == "loop_detection" and isinstance(response, dict):
        total_groups = response.get("total_groups")
        self_loops = response.get("self_loops", []) or []
        non_self_loops = response.get("non_self_loops", []) or []

        st.subheader("Loop detection summary")
        if total_groups is not None:
            st.metric("Total groups", total_groups)
        st.metric("Self-loop patterns", len(self_loops))
        st.metric("Non-self-loop patterns", len(non_self_loops))

        counts = {
            "Self loops": len(self_loops),
            "Non-self loops": len(non_self_loops),
        }
        _maybe_pie_chart(counts, "Loop pattern types")

        if self_loops:
            st.subheader("Self loops")
            st.table(self_loops)

        if non_self_loops:
            st.subheader("Non-self loops")
            st.table(non_self_loops)

        loop_support = {}
        for item in self_loops + non_self_loops:
            loop_support[item.get("pattern", "?")] = float(item.get("support", 0) or 0)
        if loop_support:
            st.markdown("### Loop support by pattern")
            st.bar_chart(loop_support)

        graph_html = _build_loops_html(response)
        with st.expander("Loop visualization"):
            st.markdown(
                "Visual representation of loop transitions. Self-loops are shown as self-edges; non-self loops are displayed as directed paths. "
                "Hover over edges to see pattern support."
            )
            if graph_html:
                components_html(graph_html, height=700, scrolling=True)
            else:
                st.info("Install pyvis for a richer loop visualization, or view the loop tables above.")

        with st.expander("Raw response"):
            st.json(response)
        return

    if isinstance(response, dict) and "data" in response:
        data = response["data"]
        st.subheader(f"{method.replace('_', ' ').title()} results")
        st.table(data)

        if method == "durations":
            support_chart = {
                item.get("activity", "?"): float(item.get("avg_duration_sec", 0) or 0)
                for item in data
                if item.get("activity") is not None
            }
            count_chart = {
                item.get("activity", "?"): int(item.get("occurrence_count", 0) or 0)
                for item in data
                if item.get("activity") is not None
            }
            if support_chart:
                st.markdown("### Average duration by activity")
                st.bar_chart(support_chart)
            if count_chart:
                st.markdown("### Occurrence count by activity")
                st.line_chart(count_chart)

        if method == "directly_follows":
            support_chart = {
                f"{item.get('source','?')}→{item.get('target','?')}": float(item.get("support", 0) or 0)
                for item in data
            }
            duration_chart = {
                f"{item.get('source','?')}→{item.get('target','?')}": float(item.get("avg_duration_sec", 0) or 0)
                for item in data
            }
            if support_chart:
                st.markdown("### Support by direct follow edge")
                st.bar_chart(support_chart)
            if duration_chart:
                st.markdown("### Average duration by direct follow edge")
                st.line_chart(duration_chart)

        if method == "bottlenecks":
            impact_chart = {
                f"{item.get('source','?')}→{item.get('target','?')}": float(item.get("impact_score", 0) or 0)
                for item in data
            }
            if impact_chart:
                st.markdown("### Impact score by activity pair (avg duration × occurrence count)")
                st.bar_chart(impact_chart)
            flagged = [item for item in data if str(item.get("flagged")).lower() == "true"]
            if flagged:
                st.markdown(f"### Flagged as anomalous ({len(flagged)})")
                st.table(flagged)

        if method == "temporal_deviations":
            st.markdown("### Duration-based discriminating rules")
            sorted_rows = sorted(data, key=lambda r: abs(float(r.get("balance", 0) or 0)), reverse=True)
            display_cols = ["rule", "balance", "confidence_1", "confidence_0", "support", "direction"]
            st.table([{k: row.get(k) for k in display_cols if k in row} for row in sorted_rows])

        with st.expander("Raw response"):
            st.json(response)
        return

    if method == "attribute_deviations":
        # HTML output: api_post falls back to {"status_code": 200, "text": "<html...>"}
        if isinstance(response, dict) and response.get("status_code") == 200 and "text" in response:
            components_html(response["text"], height=800, scrolling=True)
            return
        # JSON output: {"log_name": ..., "total_deviations": ..., "deviations": [...]}
        if isinstance(response, dict) and "deviations" in response:
            st.subheader("Attribute deviations")
            st.metric("Total deviations", response.get("total_deviations", 0))
            deviations = response.get("deviations") or []
            if deviations:
                st.table(deviations)
            with st.expander("Raw response"):
                st.json(response)
            return

    format_response(response)


def _render_process_model_response(response: Any, binary_result: bytes | None) -> None:
    if binary_result is not None:
        st.success("Model generated.")
        st.image(binary_result, caption="Process model")
        st.download_button("Download PNG", data=binary_result, file_name="process_model.png", mime="image/png")
        return

    if isinstance(response, dict) and response.get("error"):
        format_response(response)
        return

    if isinstance(response, dict) and response.get("status_code") and response["status_code"] >= 400:
        format_response(response)
        return

    if isinstance(response, dict) and response.get("status_code") == 200 and "text" in response:
        text = response["text"]
        if "<html" in text.lower():
            components_html(text, height=800, scrolling=True)
        else:
            st.download_button("Download model", data=text, file_name="process_model", mime="text/plain")
            st.code(text[:5000] + ("…" if len(text) > 5000 else ""), language="xml")
        return

    format_response(response)


def render(base_url: str) -> None:
    if "analyser_running" not in st.session_state:
        st.session_state.analyser_running = False
    if "analyser_submit_requested" not in st.session_state:
        st.session_state.analyser_submit_requested = False

    st.title("🧠 Analyser")
    st.markdown(
        "Run analysis methods on indexed logs: process model discovery, loop detection, "
        "bottleneck detection, temporal deviation analysis, plus core stats (directly-follows, "
        "durations, attribute deviations) and comparison methods (n-grams, rare/targeted rules)."
    )

    category = st.selectbox("Category", list(CATEGORIES.keys()))
    method = st.selectbox("Method", CATEGORIES[category])

    # Kept outside the form so picking a namespace immediately refreshes the log
    # dropdown's options - form widgets only rerun the script on submit.
    storage_namespace = st.selectbox("Storage namespace", namespace_options(), accept_new_options=True)
    log_name = st.selectbox("Log name", log_options(storage_namespace), accept_new_options=True)

    with st.form("analyser_form"):
        if method == "directly_follows":
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
            end_time = parse_optional(st.text_input("End time attribute", ""))
            support_threshold = parse_optional_float(st.text_input("Support threshold", ""))
            filter_out = st.checkbox("Filter out rare items", value=False)
            include_traces = st.checkbox("Include trace IDs", value=False)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "min_timestamp": min_timestamp,
                "end_time": end_time,
                "support_threshold": support_threshold,
                "filter_out": filter_out,
                "include_traces": include_traces,
                "return_csv": False,
                "output_path": f"output/{log_name}_directly_follows",
            }

        elif method == "durations":
            duration_mode = st.selectbox("Duration mode", ["activity", "group"], index=0)
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
            end_time = parse_optional(st.text_input("End time attribute", ""))
            grouping_key = parse_comma_list(st.text_input("Grouping key(s)", ""))
            grouping_value = parse_comma_list(st.text_input("Grouping value(s)", ""))
            per_group = st.checkbox("Per group output", value=False)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "duration_mode": duration_mode,
                "min_timestamp": min_timestamp,
                "end_time": end_time,
                "grouping_key": grouping_key,
                "grouping_value": grouping_value,
                "per_group": per_group,
                "return_csv": False,
                "output_path": f"output/{log_name}_durations",
            }

        elif method == "loop_detection":
            grouping_key = parse_comma_list(st.text_input("Grouping key(s)", ""))
            grouping_value = parse_comma_list(st.text_input("Grouping value(s)", ""))
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
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
                "return_csv": False,
                "output_path": f"output/{log_name}_loop_detection",
            }

        elif method == "attribute_deviations":
            _STEP_LABELS = {
                "0 – Value frequency (inter + intra-trace)": 0,
                "1 – Activity × Attribute anomalies": 1,
                "2 – Position-conditioned anomalies": 2,
                "3 – N-gram context anomalies": 3,
                "4 – Value transitions (categorical)": 4,
            }
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
            selected_step_labels = st.multiselect(
                "Steps to run",
                list(_STEP_LABELS.keys()),
                default=list(_STEP_LABELS.keys()),
            )
            steps = [_STEP_LABELS[lbl] for lbl in selected_step_labels]
            excluded_attributes = parse_comma_list(st.text_input("Excluded attributes", ""))
            surprise_threshold = st.number_input("Surprise threshold", value=4.0, format="%.2f")
            zscore_threshold = st.number_input("Z-score threshold", value=3.5, format="%.2f")
            ngram_n = st.number_input("N-gram length", value=2, min_value=1)
            min_group_size = st.number_input("Minimum group size", value=5, min_value=1)
            n_buckets = st.number_input("N buckets", value=5, min_value=1)
            support_threshold = parse_optional_float(st.text_input("Support threshold", ""))
            filter_out = st.checkbox("Filter out by support", value=False)
            on_rare = parse_optional_float(st.text_input("Rare-mode support threshold", ""))
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "min_timestamp": min_timestamp,
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
                "output_format": "html",
                "output_path": f"output/{log_name}_analyser_results",
            }

        elif method in ("ngrams", "rare_rules", "targeted_rules"):
            separating_key = st.text_input("Separating key", "activity")
            separating_groups_text = st.text_area(
                "Separating groups (one comma-separated group per line)",
                value="",
                height=120,
            )
            support_threshold = st.number_input(
                "Support threshold", value=0.0 if method == "ngrams" else (0.1 if method == "rare_rules" else 0.8),
                min_value=0.0, max_value=1.0, format="%.2f",
            )
            method_params: dict = {}
            if method == "ngrams":
                n = st.number_input("N-gram length", value=2, min_value=1)
                vis = st.checkbox("Generate visualization", value=False)
                method_params = {"n": n, "vis": vis}
            elif method == "targeted_rules":
                target_label = st.number_input("Target label", value=1, min_value=0)
                filtering_support = st.number_input(
                    "Filtering support", value=1.0, min_value=0.0, max_value=1.0, format="%.2f",
                )
                method_params = {"target_label": target_label, "filtering_support": filtering_support}
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "method_params": method_params,
                "separating_key": separating_key,
                "separating_groups": parse_group_definitions(separating_groups_text),
                "support_threshold": support_threshold,
                "output_path": f"output/{log_name}_{method}",
            }

        elif method in PROCESS_MODEL_METHODS:
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
            end_time = parse_optional(st.text_input("End time attribute (for duration annotations)", ""))
            noise_threshold = st.number_input("Noise threshold", value=0.0, min_value=0.0, max_value=1.0, format="%.2f")
            output_format = st.selectbox("Output format", ["model", "png", "html"], index=0)
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "min_timestamp": min_timestamp,
                "end_time": end_time,
                "noise_threshold": noise_threshold,
                "output_format": output_format,
                "output_path": f"output/{log_name}_{method}",
            }

        elif method == "bottlenecks":
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
            end_time = parse_optional(st.text_input("End time attribute", ""))
            grouping_key = parse_comma_list(st.text_input("Grouping key(s)", ""))
            grouping_value = parse_comma_list(st.text_input("Grouping value(s)", ""))
            zscore_threshold = st.number_input("Z-score threshold", value=3.5, format="%.2f")
            top_k = parse_optional_int(st.text_input("Top K pairs", ""))
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "min_timestamp": min_timestamp,
                "end_time": end_time,
                "grouping_key": grouping_key,
                "grouping_value": grouping_value,
                "zscore_threshold": zscore_threshold,
                "top_k": top_k,
                "return_csv": False,
                "output_path": f"output/{log_name}_bottlenecks",
            }

        else:  # temporal_deviations
            min_timestamp = parse_optional(st.text_input("Minimum timestamp (ISO 8601)", ""))
            separating_key = st.text_input("Separating key", "activity")
            separating_groups_text = st.text_area(
                "Separating groups (one comma-separated group per line, e.g. the label-1 value(s))",
                value="",
                height=100,
            )
            activity_pairs_text = st.text_area(
                "Explicit activity pairs (one 'source,target' pair per line; leave empty to auto-derive)",
                value="",
                height=100,
            )
            max_auto_pairs = st.number_input("Max auto-derived pairs", value=50, min_value=1)
            min_group_size = st.number_input("Minimum group size", value=5, min_value=1)
            top_k_per_pair = parse_optional_int(st.text_input("Top K thresholds per pair", ""))
            analyser_config = {
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "min_timestamp": min_timestamp,
                "separating_key": separating_key,
                "separating_groups": parse_group_definitions(separating_groups_text),
                "activity_pairs": parse_group_definitions(activity_pairs_text) or None,
                "max_auto_pairs": max_auto_pairs,
                "min_group_size": min_group_size,
                "top_k_per_pair": top_k_per_pair,
                "return_csv": False,
                "output_path": f"output/{log_name}_temporal_deviations",
            }

        submit = st.form_submit_button("Run analyser", disabled=st.session_state.analyser_running, key="run_analyser_button")
        if submit:
            st.session_state.analyser_submit_requested = True

    if st.session_state.analyser_submit_requested and not st.session_state.analyser_running:
        st.session_state.analyser_running = True
        st.session_state.analyser_submit_requested = False
        try:
            with st.spinner("Running analyser..."):
                if method in PROCESS_MODEL_METHODS and analyser_config.get("output_format") == "png":
                    binary_result = api_post_binary(f"analyser/{method}", base_url, payload=analyser_config)
                    _render_analyser_response(None, method, binary_result=binary_result)
                else:
                    response = api_post(f"analyser/{method}", base_url, payload=analyser_config)
                    _render_analyser_response(response, method)
        finally:
            st.session_state.analyser_running = False

    with st.expander("Need help?"):
        st.markdown(
            "- Pick a category, then a method to update the form fields automatically.\n"
            "- Use empty values for optional settings to keep the default backend behavior.\n"
            "- **Comparison** and **Temporal deviations** methods split traces into two groups via "
            "`separating_key`/`separating_groups` - each line of the text area is one comma-separated "
            "group of values that count as label 1; everything else is label 0.\n"
            "- **Process Model** methods return a native model file by default (`model`); switch to "
            "`png`/`html` for a rendered visualization."
        )

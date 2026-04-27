"""Query module page."""

import streamlit as st

from common import api_post, format_response


def render_query_response(response: dict, method: str) -> None:
    if response.get("error") or (response.get("status_code") and response["status_code"] >= 400):
        format_response(response)
        return

    code = response.get("code")
    message = response.get("message")
    if code is not None:
        if code == 200:
            st.success(f"Success ({code})")
            if message:
                st.markdown(f"**{message}**")
        else:
            st.error(f"Response code: {code}")
            if message:
                st.markdown(f"**{message}**")

    if method == "detection" and isinstance(response.get("detected"), list):
        st.subheader("Detection results")
        if response.get("time") is not None:
            try:
                elapsed = float(response["time"])
                st.metric("Elapsed time", f"{elapsed:.2f}s")
            except (TypeError, ValueError):
                st.caption(f"Processed in {response['time']} seconds")

        total = response.get("total", len(response.get("detected", [])))
        st.metric("Detected traces", total)

        detected = response["detected"]
        rows = []
        for item in detected:
            rows.append(
                {
                    "trace_id": item.get("trace_id"),
                    "support": item.get("support"),
                    "positions": ", ".join(str(p) for p in item.get("positions", [])),
                }
            )
        if rows:
            st.table(rows)
            support_chart = {row["trace_id"]: row["support"] for row in rows if row["trace_id"] is not None}
            if support_chart:
                st.bar_chart(support_chart)
        return

    if method == "exploration" and isinstance(response.get("explored"), list):
        st.subheader("Exploration results")
        if response.get("time") is not None:
            try:
                elapsed = float(response["time"])
                st.metric("Elapsed time", f"{elapsed:.2f}s")
            except (TypeError, ValueError):
                st.caption(f"Processed in {response['time']} seconds")
                
        explored = response["explored"]
        rows = []
        for item in explored:
            rows.append(
                {
                    "next_activity": item.get("next_activity"),
                    "support": item.get("support"),
                }
            )
        if rows:
            st.table(rows)
            support_chart = {row["next_activity"]: row["support"] for row in rows if row["next_activity"] is not None}
            if support_chart:
                st.bar_chart(support_chart)


        return

    if method == "statistics" and isinstance(response, dict):
        stats_rows = []
        for pattern, details in response.items():
            if pattern == "code" or pattern == "message":
                continue
            if isinstance(details, dict):
                row = {"pattern": pattern}
                row.update({k: v for k, v in details.items() if k != "code"})
                stats_rows.append(row)
        if stats_rows:
            st.subheader("Statistics summary")
            if response.get("time") is not None:
                try:
                    elapsed = float(response["time"])
                    st.metric("Elapsed time", f"{elapsed:.2f}s")
                except (TypeError, ValueError):
                    st.caption(f"Processed in {response['time']} seconds")

            st.table(stats_rows)
            durations = {
                row["pattern"]: row["total_duration"]
                for row in stats_rows
                if row.get("total_duration") is not None
            }
            completions = {
                row["pattern"]: row["total_completions"]
                for row in stats_rows
                if row.get("total_completions") is not None
            }
            if durations:
                st.bar_chart(durations)
            if completions:
                st.line_chart(completions)
            return

    format_response(response)


def render(base_url: str) -> None:
    if "query_running" not in st.session_state:
        st.session_state.query_running = False
    if "query_submit_requested" not in st.session_state:
        st.session_state.query_submit_requested = False

    st.title("🔍 Query Executor")
    st.markdown("Build query patterns for statistics, detection, or exploration on indexed logs.")

    method = st.selectbox("Query method", ["statistics", "detection", "exploration"])

    explore_mode = "accurate"
    if method == "exploration":
        explore_mode = st.selectbox("Explore mode", ["accurate", "fast", "hybrid"], index=0)

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
        explore_k = 0

        if method == "exploration":
            explore_k = st.number_input(
                "Explore K",
                value=10,
                min_value=2,
                disabled=explore_mode == "accurate",
                help="Number of candidates to explore (not used in accurate mode).",
            )
            if explore_mode == "accurate":
                explore_k = 0

        submit = st.form_submit_button("Run query", disabled=st.session_state.query_running, key="run_query_button")
        if submit:
            st.session_state.query_submit_requested = True

    if st.session_state.query_submit_requested and not st.session_state.query_running:
        st.session_state.query_running = True
        st.session_state.query_submit_requested = False
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
        with st.spinner("Running query..."):
            response = api_post(f"querying/{method}", base_url, payload=query_config)
            if isinstance(response, dict):
                render_query_response(response, method)
            else:
                format_response(response)
        st.session_state.query_running = False

    with st.expander("Need help?"):
        st.markdown(
            "- `statistics` and `detection` use the current query pattern directly.\n"
            "- `exploration` uses the pattern as a seed and can return similar matches.\n"
            "- Keep support thresholds low for exploratory analysis and higher for precise detection."
        )

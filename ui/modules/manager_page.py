"""Log Manager module page."""

import streamlit as st

from common import api_delete, api_get, api_post, format_response, log_options, namespace_options


def _render_namespaces_response(response) -> None:
    if response is None:
        return

    if response.get("error") or (response.get("status_code") and response["status_code"] >= 400):
        format_response(response)
        return

    namespaces = response.get("namespaces")
    if isinstance(namespaces, list):
        st.metric("Namespace count", response.get("namespace_count", len(namespaces)))
        rows = [
            {"namespace": ns.get("name"), "log_count": ns.get("log_count"), "logs": ", ".join(ns.get("logs", []))}
            for ns in namespaces
        ]
        if rows:
            st.table(rows)
        else:
            st.info("No namespaces found.")
        with st.expander("Raw response"):
            st.json(response)
        return

    format_response(response)


def _namespaces_tab(base_url: str) -> None:
    st.subheader("Storage namespaces")
    st.write("List every storage namespace and the logs stored within each of them.")

    if st.button("Refresh namespaces", key="manager_refresh_namespaces"):
        st.session_state.manager_namespaces = api_get("manager/namespaces", base_url)

    _render_namespaces_response(st.session_state.get("manager_namespaces"))

    st.markdown("---")
    st.subheader("Delete a namespace")
    st.warning("This permanently deletes the namespace and every log stored within it.")
    namespace_to_delete = st.selectbox(
        "Storage namespace to delete",
        namespace_options(),
        accept_new_options=True,
        key="manager_delete_namespace_select",
    )
    with st.form("manager_delete_namespace_form"):
        confirm = st.checkbox("I understand this action is irreversible.")
        submit = st.form_submit_button("Delete namespace")
        if submit:
            if not namespace_to_delete.strip():
                st.error("Enter a storage namespace to delete.")
            elif not confirm:
                st.error("Confirm the deletion before proceeding.")
            else:
                result = api_delete(
                    "manager/delete_namespace",
                    base_url,
                    params={"storage_namespace": namespace_to_delete.strip()},
                )
                format_response(result)


def _metadata_tab(base_url: str) -> None:
    st.subheader("Log metadata")
    # Kept outside the form so picking a namespace immediately refreshes the log
    # dropdown's options - form widgets only rerun the script on submit.
    storage_namespace = st.selectbox(
        "Storage namespace", namespace_options(), accept_new_options=True, key="manager_metadata_namespace",
    )
    log_name = st.selectbox(
        "Log name", log_options(storage_namespace), accept_new_options=True, key="manager_metadata_log_name",
    )
    with st.form("manager_metadata_form"):
        include_alphabet = st.checkbox("Include activity alphabet", value=False)
        include_indexed_pairs = st.checkbox("Include indexed activity pairs", value=False)
        submit = st.form_submit_button("Fetch metadata")

    if submit:
        response = api_get(
            "manager/log_metadata",
            base_url,
            params={
                "log_name": log_name,
                "storage_namespace": storage_namespace,
                "include_alphabet": include_alphabet,
                "include_indexed_pairs": include_indexed_pairs,
            },
        )
        format_response(response)

    st.markdown("---")
    st.subheader("Delete a log")
    st.warning("This permanently deletes the log and all of its indexed tables from the namespace.")
    del_namespace = st.selectbox(
        "Storage namespace", namespace_options(), accept_new_options=True, key="manager_delete_log_namespace",
    )
    del_log_name = st.selectbox(
        "Log name to delete", log_options(del_namespace), accept_new_options=True, key="manager_delete_log_name",
    )
    with st.form("manager_delete_log_form"):
        confirm = st.checkbox("I understand this action is irreversible.", key="manager_delete_log_confirm")
        submit_delete = st.form_submit_button("Delete log")
        if submit_delete:
            if not del_log_name.strip():
                st.error("Enter a log name to delete.")
            elif not confirm:
                st.error("Confirm the deletion before proceeding.")
            else:
                result = api_delete(
                    "manager/delete_log",
                    base_url,
                    params={"log_name": del_log_name.strip(), "storage_namespace": del_namespace},
                )
                format_response(result)


def _tables_tab(base_url: str) -> None:
    st.subheader("Available tables")
    storage_namespace = st.selectbox(
        "Storage namespace", namespace_options(), accept_new_options=True, key="manager_tables_namespace",
    )
    log_name = st.selectbox(
        "Log name", log_options(storage_namespace), accept_new_options=True, key="manager_tables_log_name",
    )
    with st.form("manager_tables_form"):
        submit = st.form_submit_button("List tables")

    if submit:
        response = api_get(
            "manager/tables",
            base_url,
            params={"log_name": log_name, "storage_namespace": storage_namespace},
        )
        if isinstance(response, dict) and isinstance(response.get("tables"), list):
            st.table([{"table": t} for t in response["tables"]])
            with st.expander("Raw response"):
                st.json(response)
        else:
            format_response(response)


def _query_tab(base_url: str) -> None:
    st.subheader("Ad-hoc SQL query")
    st.write(
        "Run a read-only SQL statement (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN) against a log's tables. "
        "Use the **Tables** tab to see available table names."
    )
    storage_namespace = st.selectbox(
        "Storage namespace", namespace_options(), accept_new_options=True, key="manager_query_namespace",
    )
    log_name = st.selectbox(
        "Log name", log_options(storage_namespace), accept_new_options=True, key="manager_query_log_name",
    )
    with st.form("manager_query_form"):
        sql = st.text_area(
            "SQL",
            "SELECT activity, COUNT(*) AS event_count FROM activity_index GROUP BY activity ORDER BY event_count DESC",
            height=120,
        )
        row_limit = st.number_input("Row limit", value=1000, min_value=1)
        submit = st.form_submit_button("Run query")

    if submit:
        query_config = {
            "log_name": log_name,
            "storage_namespace": storage_namespace,
            "sql": sql,
            "row_limit": row_limit,
        }
        response = api_post("manager/query", base_url, payload=query_config)
        if isinstance(response, dict) and isinstance(response.get("rows"), list):
            st.metric("Row count", response.get("row_count", len(response["rows"])))
            if response["rows"]:
                st.table(response["rows"])
            else:
                st.info("Query returned no rows.")
            if response.get("available_tables"):
                st.caption("Available tables: " + ", ".join(response["available_tables"]))
            with st.expander("Raw response"):
                st.json(response)
        else:
            format_response(response)


def render(base_url: str) -> None:
    st.title("🗄️ Log Manager")
    st.markdown(
        "Browse storage namespaces and logs, inspect metadata, run ad-hoc SQL queries, "
        "and delete logs or namespaces you no longer need."
    )

    tabs = st.tabs(["Namespaces", "Log metadata", "Tables", "Ad-hoc query"])
    with tabs[0]:
        _namespaces_tab(base_url)
    with tabs[1]:
        _metadata_tab(base_url)
    with tabs[2]:
        _tables_tab(base_url)
    with tabs[3]:
        _query_tab(base_url)

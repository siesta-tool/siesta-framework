"""Miner module page."""

import json

import streamlit as st
from streamlit.components.v1 import html as components_html

from common import api_post, format_response


CATEGORY_COLORS = {
    "positional": "blue",
    "existential": "green",
    "ordered": "orange",
    "unordered": "purple",
    "negation": "red",
}


def _rule_color(category: str) -> str:
    return CATEGORY_COLORS.get(category, "black")


def _build_rule_graph(mined: list[dict]) -> str:
    lines = ["digraph mined_rules {", "rankdir=LR;", "node [shape=circle, style=filled, fillcolor=lightgray];"]
    node_names = set()

    for item in mined:
        category = item.get("category", "")
        source = item.get("source", "") or "?"
        target = item.get("target", "")
        template = item.get("template", "")
        support = item.get("support", "")
        color = _rule_color(category)

        node_names.add(source)
        if target:
            node_names.add(target)
            label = f"{template} ({support})"
            lines.append(
                f'"{source}" -> "{target}" [label="{label}", color="{color}", fontcolor="{color}"]'
            )
        else:
            lines.append(
                f'"{source}" [label="{source}\n{template} ({support})", fillcolor="{color}", fontcolor="white"]'
            )

    if not node_names:
        lines.append("empty [label=\"No rules to visualize\", shape=plaintext]")
    lines.append("}")
    return "\n".join(lines)


def _build_pyvis_html(mined: list[dict]) -> str | None:
    try:
        from pyvis.network import Network
    except ImportError:
        return None

    net = Network(height="650px", width="100%", directed=True)
    net.toggle_physics(True)
    net.barnes_hut()

    for item in mined:
        category = item.get("category", "")
        source = item.get("source", "") or "?"
        target = item.get("target", "")
        template = item.get("template", "")
        support = item.get("support", "")
        color = _rule_color(category)

        title_text = (
            f"Category: {category} Template: {template} Support: {support} Source: {source}"
        )
        net.add_node(source, label=source, title=title_text, color=color)

        if target:
            target_title = (
                f"Category: {category} Template: {template} Support: {support} Target: {target}"
            )
            net.add_node(target, label=target, title=target_title, color=color)
            edge_label = f"{template} ({support})"
            net.add_edge(source, target, label=edge_label, title=edge_label, color=color)

    html = net.generate_html()
    return html


def render_miner_response(response: dict) -> None:
    if response.get("error") or (response.get("status_code") and response["status_code"] >= 400):
        format_response(response)
        return

    code = response.get("code")
    if code is not None:
        if code == 200:
            st.success(f"Success ({code})")
            if response.get("message"):
                st.markdown(f"**{response['message']}**")
        else:
            st.error(f"Response code: {code}")
            if response.get("message"):
                st.markdown(f"**{response['message']}**")

    if response.get("time") is not None:
        try:
            elapsed = float(response["time"])
            st.metric("Elapsed time", f"{elapsed:.2f}s")
        except (TypeError, ValueError):
            st.caption(f"Elapsed time: {response['time']}")

    mined = response.get("mined")
    if code is not None and code != 200 and not isinstance(mined, list):
        st.json(response)
        return

    if isinstance(mined, list):
        st.subheader("Mined rules")
        st.metric("Total rules", len(mined))

        rows = []
        for item in mined:
            rows.append(
                {
                    "category": item.get("category", ""),
                    "template": item.get("template", ""),
                    "source": item.get("source", ""),
                    "target": item.get("target", ""),
                    "support": item.get("support", ""),
                    "occurrences": item.get("occurrences", ""),
                }
            )

        min_support = st.slider(
            "Filter rules by minimum support",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.05,
            key="miner_support_filter",
            help="Only show rules with support equal to or larger than this threshold.",
        )

        filtered_rows = []
        for row in rows:
            try:
                support_val = float(row["support"])
            except (TypeError, ValueError):
                support_val = 0.0
            if support_val >= min_support:
                filtered_rows.append(row)

        if filtered_rows:
            st.table(filtered_rows)
            supports = {}
            for row in filtered_rows:
                support_val = float(row["support"])
                supports[f"{row['source']}→{row['target']} ({row['template']})"] = support_val

            if supports:
                st.bar_chart(supports)
        elif rows:
            st.info(f"No rules match support threshold >= {min_support:.2f}.")

        filtered_mined = []
        for item in mined:
            try:
                support_val = float(item.get("support", 0))
            except (TypeError, ValueError):
                support_val = 0.0
            if support_val >= min_support:
                filtered_mined.append(item)

        graph_html = _build_pyvis_html(filtered_mined)
        with st.expander("Rule visualization"):
            st.markdown(
                "Each node represents an activity value. "
                "Edges represent rules between a source and target activity, labelled with the rule template and support. "
                "If a rule has no target, the node itself shows the template and support for that activity. "
                "Edge color indicates the rule category."
            )
            if graph_html:
                components_html(graph_html, height=700, scrolling=True)
            else:
                graph_code = _build_rule_graph(filtered_mined)
                st.graphviz_chart(graph_code)

        with st.expander("Raw response"):
            st.json(response)
        return

    format_response(response)


def render(base_url: str) -> None:
    if "miner_running" not in st.session_state:
        st.session_state.miner_running = False
    if "miner_submit_requested" not in st.session_state:
        st.session_state.miner_submit_requested = False
    if "miner_response" not in st.session_state:
        st.session_state.miner_response = None

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
            use_window_grouping = st.checkbox(
                "Enable window grouping",
                value=False,
                help="Activate window grouping and enable the window size field."
            )
            grouping_options = ["trace"]
            if use_window_grouping:
                grouping_options.append("window")
            grouping = st.selectbox("Grouping", grouping_options, index=0)

        with col2:
            window_size = st.number_input(
                "Window size",
                value=30,
                min_value=1,
                disabled=not use_window_grouping,
                help="Only active when window grouping is enabled.",
            )
            support_threshold = st.number_input(
                "Support threshold",
                value=0.0,
                min_value=0.0,
                max_value=1.0,
                format="%.2f",
            )
            include_trace_lists = st.checkbox("Include trace lists", value=False)
            force_recompute = st.checkbox("Force recompute", value=False)

        submit = st.form_submit_button("Run miner", disabled=st.session_state.miner_running, key="run_miner_button")
        if submit:
            st.session_state.miner_submit_requested = True

    if st.session_state.miner_submit_requested and not st.session_state.miner_running:
        st.session_state.miner_running = True
        st.session_state.miner_submit_requested = False
        mining_config = {
            "log_name": log_name,
            "storage_namespace": storage_namespace,
            "categories": categories,
            "grouping": grouping,
            "window_size": window_size,
            "support_threshold": support_threshold,
            "include_trace_lists": include_trace_lists,
            "force_recompute": force_recompute,
            "output_path": f"output/{log_name}",
        }
        with st.spinner("Running miner..."):
            response = api_post("mining/run", base_url, payload=mining_config)
            st.session_state.miner_response = response if isinstance(response, dict) else {"error": "Unexpected non-json response"}
        st.session_state.miner_running = False

    if st.session_state.miner_response is not None:
        render_miner_response(st.session_state.miner_response)

    with st.expander("Need help?"):
        st.markdown(
            "- Use `categories` to narrow the mining output by constraint type.\n"
            "- `Force recompute` overrides any cached results on the backend.\n"
            "- `Include trace lists` can produce more detailed results at the cost of larger responses."
        )

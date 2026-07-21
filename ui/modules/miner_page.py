"""Miner module page."""

import hashlib
import json
import re
from pathlib import Path

import streamlit as st
from streamlit.components.v1 import html as components_html

from common import api_post, format_response, log_options, namespace_options


CATEGORY_COLORS = {
    "positional": "blue",
    "existential": "green",
    "ordered": "orange",
    "unordered": "purple",
    "negation": "red",
}

RULES_PAGE_SIZE = 50
MAX_VISUALIZED_RULES = 300


def _rule_color(category: str) -> str:
    return CATEGORY_COLORS.get(category, "black")


def _to_number(value: str) -> float | None:
    """Coerce a CSV-sourced numeric field to float, treating "" (e.g. interest on
    positional/existential rules, which have no target) as missing rather than as a
    string - a column mixing numeric and empty strings breaks st.dataframe's Arrow
    conversion."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _load_local_vis_assets() -> tuple[str, str] | None:
    """Locate pyvis's own bundled vis-network JS/CSS, so the graph doesn't depend on
    a CDN. If that CDN is slow or blocked (e.g. restricted egress from the container
    this UI runs in), the iframe just hangs waiting for it - which looks exactly like
    a freeze, and wouldn't be fixed by anything on the layout/physics side."""
    try:
        import pyvis
    except ImportError:
        return None

    lib_dir = Path(pyvis.__file__).parent / "lib"
    for candidate in sorted(lib_dir.glob("vis-*"), reverse=True):
        js_path = candidate / "vis-network.min.js"
        css_path = candidate / "vis-network.css"
        if js_path.exists() and css_path.exists():
            return js_path.read_text(), css_path.read_text()
    return None


def _inline_vis_assets(html: str) -> str:
    assets = _load_local_vis_assets()
    if assets:
        vis_js, vis_css = assets
        html = re.sub(
            r'<link rel="stylesheet" href="https://cdnjs\.cloudflare\.com/ajax/libs/vis-network/[^"]+"[^>]*/>',
            lambda _match: f"<style>{vis_css}</style>",
            html,
        )
        html = re.sub(
            r'<script src="https://cdnjs\.cloudflare\.com/ajax/libs/vis-network/[^"]+"[^>]*></script>',
            lambda _match: f"<script>{vis_js}</script>",
            html,
        )

    # Drop dependencies that don't matter here: a relative path only valid when pyvis
    # writes a companion lib/ folder to disk (not when embedded as a raw HTML string),
    # and Bootstrap, which only styles chrome (buttons, cards) this view doesn't use.
    html = re.sub(r'<script src="lib/bindings/utils\.js"></script>', "", html)
    html = re.sub(r'<link\s[^>]*jsdelivr\.net/npm/bootstrap[^>]*/>', "", html)
    html = re.sub(r'<script\s[^>]*jsdelivr\.net/npm/bootstrap[^>]*></script>', "", html)
    return html


def _build_pyvis_html(mined: list[dict]) -> str | None:
    try:
        from pyvis.network import Network
    except ImportError:
        return None

    net = Network(height="650px", width="100%", directed=True)
    # A force-directed layout (pyvis's default) runs an iterative physics simulation
    # on the main thread - even bounded to a modest iteration count, that's still a
    # blocking computation whose cost grows with graph size, and it's what freezes
    # the page for anything but a handful of nodes. A hierarchical layout is a single
    # deterministic pass with no simulation loop, so there's no freeze risk at all
    # regardless of how many rules are visualized.
    net.set_options("""
    {
      "layout": {
        "hierarchical": {
          "enabled": true,
          "direction": "LR",
          "sortMethod": "directed",
          "levelSeparation": 150,
          "nodeSpacing": 120
        }
      },
      "physics": {
        "enabled": false
      },
      "edges": {
        "smooth": {
          "enabled": true,
          "type": "cubicBezier"
        }
      }
    }
    """)

    for item in mined:
        category = item.get("category", "")
        source = item.get("source", "") or "?"
        target = item.get("target", "")
        template = item.get("template", "")
        support = item.get("support", "")
        confidence = item.get("confidence", "")
        interest = item.get("interest", "")
        color = _rule_color(category)

        title_text = (
            f"Category: {category} Template: {template} Support: {support} "
            f"Confidence: {confidence} Interest: {interest} Source: {source}"
        )
        net.add_node(source, label=source, title=title_text, color=color)

        if target:
            target_title = (
                f"Category: {category} Template: {template} Support: {support} "
                f"Confidence: {confidence} Interest: {interest} Target: {target}"
            )
            net.add_node(target, label=target, title=target_title, color=color)
            edge_label = f"{template} ({support})"
            edge_title = f"{edge_label} - confidence: {confidence}, interest: {interest}"
            net.add_edge(source, target, label=edge_label, title=edge_title, color=color)

    html = net.generate_html()
    return _inline_vis_assets(html)


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
                    "occurrences": _to_number(item.get("occurrences", "")),
                    "support": _to_number(item.get("support", "")),
                    "confidence": _to_number(item.get("confidence", "")),
                    "interest": _to_number(item.get("interest", "")),
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
            total_pages = max(1, (len(filtered_rows) + RULES_PAGE_SIZE - 1) // RULES_PAGE_SIZE)
            page_key = "miner_rules_page"
            if page_key not in st.session_state:
                st.session_state[page_key] = 1
            elif st.session_state[page_key] > total_pages:
                st.session_state[page_key] = total_pages

            if total_pages > 1:
                page = st.number_input(
                    "Page", min_value=1, max_value=total_pages, step=1, key=page_key,
                    help=f"{len(filtered_rows)} rules match; {RULES_PAGE_SIZE} shown per page.",
                )
            else:
                page = 1

            start = (page - 1) * RULES_PAGE_SIZE
            page_rows = filtered_rows[start:start + RULES_PAGE_SIZE]
            st.caption(f"Showing rules {start + 1}-{start + len(page_rows)} of {len(filtered_rows)}")
            st.dataframe(page_rows, width="stretch")

            supports = {}
            for row in page_rows:
                support_val = float(row["support"])
                supports[f"{row['source']}→{row['target']} ({row['template']})"] = support_val

            if supports:
                st.bar_chart(supports)
        elif rows:
            st.info(f"No rules match support threshold >= {min_support:.2f}.")

        show_visualization = st.checkbox(
            "Show rule visualization",
            value=False,
            key="miner_show_visualization",
            help="Builds a network diagram of the currently filtered rules. Off by default since it's "
            "expensive to (re)build and can lag the page for large rule sets.",
        )
        if show_visualization:
            filtered_mined = []
            for item in mined:
                try:
                    support_val = float(item.get("support", 0))
                except (TypeError, ValueError):
                    support_val = 0.0
                if support_val >= min_support:
                    filtered_mined.append(item)

            visualized_mined = filtered_mined[:MAX_VISUALIZED_RULES]
            if len(filtered_mined) > MAX_VISUALIZED_RULES:
                st.caption(
                    f"Visualization limited to the first {MAX_VISUALIZED_RULES} of {len(filtered_mined)} "
                    "filtered rules for performance. Raise the support filter to narrow further."
                )

            st.markdown(
                "Each node represents an activity value. "
                "Edges represent rules between a source and target activity, labelled with the rule template and support. "
                "If a rule has no target, the node itself shows the template and support for that activity. "
                "Edge color indicates the rule category."
            )

            # Streamlit reruns this whole function on any widget interaction, even ones
            # unrelated to the visualization (e.g. changing the table page). Rebuilding
            # and re-embedding the network on every one of those is what actually makes
            # the page feel frozen, so only rebuild when the visualized rule set changes.
            cache_key = hashlib.sha1(
                json.dumps(visualized_mined, sort_keys=True, default=str).encode()
            ).hexdigest()
            if st.session_state.get("miner_graph_cache_key") != cache_key:
                st.session_state["miner_graph_cache_key"] = cache_key
                st.session_state["miner_graph_html"] = _build_pyvis_html(visualized_mined)
                st.session_state["miner_graph_code"] = _build_rule_graph(visualized_mined)

            graph_html = st.session_state["miner_graph_html"]
            if graph_html:
                components_html(graph_html, height=700, scrolling=True)
            else:
                st.graphviz_chart(st.session_state["miner_graph_code"])

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

    # Kept outside the form: form widgets only rerun the script on submit, so these
    # need to live outside it to dynamically enable/disable the window size field
    # and refresh the log dropdown as namespace/grouping choices change.
    storage_namespace = st.selectbox("Storage namespace", namespace_options(), accept_new_options=True)
    log_name = st.selectbox("Log name", log_options(storage_namespace), accept_new_options=True)
    use_window_grouping = st.checkbox(
        "Enable window grouping",
        value=False,
        help="Activate window grouping and enable the window size field."
    )
    enable_branching = st.checkbox(
        "Enable branching",
        value=False,
        help="Merge singular constraints that share an activity into branched ones "
        "(a set of activities combined under a logical policy). Enables the branching fields below.",
    )

    with st.form("miner_form"):
        col1, col2 = st.columns(2)
        with col1:
            categories = st.multiselect(
                "Categories",
                ["*", "positional", "existential", "ordered", "unordered", "negation"],
                default=["*"],
            )
            grouping_options = ["trace"]
            if use_window_grouping:
                grouping_options.append("window")
            grouping = st.selectbox("Grouping", grouping_options, index=0)
            window_size = st.number_input(
                "Window size",
                value=30,
                min_value=1,
                disabled=not use_window_grouping,
                help="Only active when window grouping is enabled.",
            )
            force_recompute = st.checkbox("Force recompute", value=False)

        with col2:
            support_threshold = st.number_input(
                "Support threshold",
                value=0.0,
                min_value=0.0,
                max_value=1.0,
                format="%.2f",
            )
            confidence_threshold = st.number_input(
                "Confidence threshold",
                value=0.0,
                min_value=0.0,
                max_value=1.0,
                format="%.2f",
            )
            interest_threshold = st.number_input(
                "Interest threshold",
                value=0.0,
                min_value=0.0,
                format="%.2f",
                help="Minimum interest (support relative to the independence baseline). "
                "Not defined for positional/existential rules, which are unaffected by this threshold.",
            )
            include_trace_lists = st.checkbox("Include trace lists", value=False)

        st.divider()
        st.markdown(
            "**Branching** — merge singular constraints into branched ones over a set of "
            "activities. Fields are only used when *Enable branching* is checked."
        )
        bcol1, bcol2 = st.columns(2)
        with bcol1:
            branching_type = st.selectbox(
                "Branch over",
                ["target", "source"],
                index=0,
                disabled=not enable_branching,
                help="Pair constraints (ordered/unordered) branch over the chosen side: "
                "'target' merges targets sharing a source, 'source' merges sources sharing a target. "
                "Existential/positional always extend the activity regardless.",
            )
            branching_policy = st.selectbox(
                "Policy",
                ["or", "and", "xor"],
                index=0,
                disabled=not enable_branching,
                help="Logical relation merging the satisfying trace sets: 'or' (union), "
                "'and' (intersection), 'xor' (exclusive). Positional constraints allow only or/xor.",
            )
        with bcol2:
            branching_approach = st.selectbox(
                "Approach",
                ["auto", "bottomup", "topdown"],
                index=0,
                disabled=not enable_branching,
                help="Greedy merge direction. 'auto' = top-down for OR, bottom-up for AND/XOR.",
            )
            branching_bound = st.number_input(
                "Bound",
                value=0,
                min_value=0,
                step=1,
                disabled=not enable_branching,
                help="Maximum number of activities in a branched set. 0 = unbounded "
                "(stop only when support would drop below the support threshold).",
            )

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
            "confidence_threshold": confidence_threshold,
            "interest_threshold": interest_threshold,
            "include_trace_lists": include_trace_lists,
            "force_recompute": force_recompute,
            "branching_type": branching_type if enable_branching else "none",
            "branching_policy": branching_policy,
            "branching_bound": int(branching_bound),
            "branching_approach": branching_approach,
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
            "- `Support`/`Confidence`/`Interest` thresholds are applied on the backend before results are "
            "returned; `Interest` isn't defined for positional/existential rules, so it doesn't filter them.\n"
            "- `Force recompute` overrides any cached results on the backend.\n"
            "- `Include trace lists` can produce more detailed results at the cost of larger responses.\n"
            "- `Enable branching` merges singular constraints into branched ones whose source/target is a "
            "set of activities (shown pipe-delimited, e.g. `A|B|C`). Branched rules report support only "
            "(confidence/interest are undefined for an activity set). Negations are never branched."
        )

import streamlit as st

from common import health_check
from modules import render_analyser, render_comparator, render_indexer, render_miner, render_query

MODULES = [
    {
        "key": "Indexer",
        "title": "Indexer",
        "emoji": "🧩",
        "description": "Build log indexing requests for batch or streaming ingestion.",
    },
    {
        "key": "Analyser",
        "title": "Analyser",
        "emoji": "🧠",
        "description": "Run analysis methods on indexed logs and inspect results.",
    },
    {
        "key": "Miner",
        "title": "Miner",
        "emoji": "⛏️",
        "description": "Discover declarative constraints from indexed logs.",
    },
    {
        "key": "Comparator",
        "title": "Comparator",
        "emoji": "⚖️",
        "description": "Compare log fragments with n-grams, rare rules, or targeted rules.",
    },
    {
        "key": "Query",
        "title": "Query",
        "emoji": "🔍",
        "description": "Execute statistics, detection, or exploration queries.",
    },
]


def _set_page(page: str) -> None:
    st.session_state.current_page = page


def render_home(base_url: str) -> None:
    st.title("Dashboard")
    st.markdown(
        "Siesta is an application-agnostic, open-source tool designed to build incremental indices from continuously streaming event data. These indices enable efficient analysis of the data, supporting tasks such as detecting complex patterns, predicting future events, and uncovering constraints that describe both the sequence order and temporal aspects of the events."
    )
    st.markdown("---")

    columns = st.columns(3)
    for index, module in enumerate(MODULES):
        with columns[index % 3]:
            st.markdown(f"### {module['emoji']} {module['title']}")
            st.write(module["description"])
            st.button(
                f"Open {module['title']}",
                key=f"open_{module['key']}",
                on_click=_set_page,
                args=(module["title"],),
            )

    st.markdown("---")
    st.subheader("API health")
    st.write(
        "Ping the Siesta API to verify availability before you run requests. "
    )

    if "health_ping_result" not in st.session_state:
        st.session_state.health_ping_result = None

    if st.button("Ping API health", key="home_ping"):
        st.session_state.health_ping_result = health_check(base_url)

    if st.session_state.health_ping_result is not None:
        result = st.session_state.health_ping_result
        if result.get("alive"):
            st.success(f"API reachable at {result.get('endpoint', base_url)} (HTTP {result.get('status_code')})")
            if "data" in result:
                st.json(result["data"])
            elif "text" in result:
                st.code(result["text"])
        else:
            st.error(f"Health check failed: {result.get('error', 'Unknown error')}")

    with st.expander("Quick tips"):
        st.markdown(
            "- Use the sidebar to jump quickly to any module page.\n"
            "- Fill only the fields you need and leave optional settings blank for defaults.\n"
            "- Ping the API first if you run into connectivity issues."
        )


def render_page(page_name: str, base_url: str) -> None:
    if page_name == "Indexer":
        render_indexer(base_url)
    elif page_name == "Analyser":
        render_analyser(base_url)
    elif page_name == "Miner":
        render_miner(base_url)
    elif page_name == "Comparator":
        render_comparator(base_url)
    elif page_name == "Query":
        render_query(base_url)
    else:
        render_home(base_url)


def main() -> None:
    st.set_page_config(page_title="Siesta UI", layout="wide")    
    st.sidebar.image("siesta-tool-logo.png", width="stretch")    

    page_options = ["Home"] + [module["title"] for module in MODULES]
    if "current_page" not in st.session_state:
        st.session_state.current_page = "Home"
    st.sidebar.markdown("---")

    st.sidebar.radio(
        "Navigate",
        page_options,
        key="current_page",
    )

    st.sidebar.markdown("---")

    base_url = st.sidebar.text_input("Siesta API base URL", "http://localhost:8000")
    if st.sidebar.button("Ping API health", key="sidebar_ping"):
        st.session_state.health_ping_result = health_check(base_url)

    if st.session_state.get("health_ping_result") is not None:
        result = st.session_state.health_ping_result
        if result.get("alive"):
            st.sidebar.success(f"API OK: {result.get('status_code')} @ {result.get('endpoint')}")
        else:
            st.sidebar.error(f"Health check error: {result.get('error')}")

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "MIT License 2025 | [GitHub Repo](https://github.com/siesta-tool/siesta-framework)"
    )

    render_page(st.session_state.current_page, base_url)


if __name__ == "__main__":
    main()

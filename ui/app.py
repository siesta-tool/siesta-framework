"""
Siesta Framework - Web UI

A modern, minimal interface for the Siesta event log processing framework.
"""
import streamlit as st
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Siesta Framework",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern, minimal design
st.markdown("""
<style>
    /* Main theme colors */
    :root {
        --primary-color: #6366f1;
        --secondary-color: #8b5cf6;
        --background-color: #0f172a;
        --surface-color: #1e293b;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #1e293b;
    }
    
    /* Header styling */
    h1 {
        color: #6366f1;
        font-weight: 700;
        letter-spacing: -0.02em;
    }
    
    h2, h3 {
        color: #e2e8f0;
        font-weight: 600;
    }
    
    /* Card-like containers */
    .stAlert {
        border-radius: 0.5rem;
        border: 1px solid #334155;
    }
    
    /* Button styling */
    .stButton>button {
        border-radius: 0.375rem;
        font-weight: 500;
        transition: all 0.2s;
    }
    
    .stButton>button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 600;
    }
    
    /* Progress bars */
    .stProgress > div > div {
        background-color: #6366f1;
    }
    
    /* File uploader */
    [data-testid="stFileUploader"] {
        border-radius: 0.5rem;
        border: 2px dashed #475569;
    }
    
    /* Success/Error messages */
    .element-container:has(.stSuccess) {
        border-left: 4px solid #10b981;
    }
    
    .element-container:has(.stError) {
        border-left: 4px solid #ef4444;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'api_url' not in st.session_state:
    st.session_state.api_url = os.getenv('API_BASE_URL', 'http://localhost:8000')

if 's3_config' not in st.session_state:
    st.session_state.s3_config = {
        'endpoint': os.getenv('S3_ENDPOINT', 'http://localhost:9000'),
        'access_key': os.getenv('S3_ACCESS_KEY', 'minioadmin'),
        'secret_key': os.getenv('S3_SECRET_KEY', 'minioadmin'),
        'region': os.getenv('S3_REGION', 'us-east-1')
    }

if 'default_namespace' not in st.session_state:
    st.session_state.default_namespace = os.getenv('DEFAULT_NAMESPACE', 'siesta')

# Sidebar navigation
with st.sidebar:
    st.title("⚡ Siesta Framework")
    st.markdown("---")
    
    # Navigation
    page = st.radio(
        "Navigation",
        ["🏠 Home", "🔄 Preprocess", "⛏️ Mining", "💾 Storage Browser", "⚙️ Settings"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # API Status
    st.subheader("System Status")
    from utils.api_client import SiestaAPIClient
    
    @st.cache_data(ttl=30)  # Cache for 30 seconds
    def check_api_status(api_url):
        api_client = SiestaAPIClient(api_url)
        return api_client.health_check()
    
    api_status = check_api_status(st.session_state.api_url)
    
    if api_status:
        st.success("✓ API Connected")
    else:
        st.error("✗ API Offline")
    
    st.caption(f"API: `{st.session_state.api_url}`")
    
    st.markdown("---")
    st.caption("Version 1.0.0")

# Main content area
if page == "🏠 Home":
    st.title("Welcome to Siesta Framework")
    st.markdown("""
    ### Modern Event Log Processing & Constraint Mining
    
    Siesta Framework provides powerful tools for processing event logs and mining declarative constraints
    using distributed computing with Apache Spark.
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 🔄 Preprocess")
        st.markdown("""
        - Upload event logs (XES, CSV, JSON)
        - Configure field mappings
        - Batch or streaming mode
        - Store in distributed tables
        """)
        if st.button("Go to Preprocess →", use_container_width=True):
            st.session_state.nav_override = "🔄 Preprocess"
            st.rerun()
    
    with col2:
        st.markdown("### ⛏️ Mining")
        st.markdown("""
        - Discover declarative constraints
        - Multiple constraint types
        - Configurable thresholds
        - Export results to CSV
        """)
        if st.button("Go to Mining →", use_container_width=True):
            st.session_state.nav_override = "⛏️ Mining"
            st.rerun()
    
    with col3:
        st.markdown("### 💾 Storage")
        st.markdown("""
        - Browse S3 buckets
        - View log metadata
        - Inspect table structures
        - Monitor storage usage
        """)
        if st.button("Go to Storage →", use_container_width=True):
            st.session_state.nav_override = "💾 Storage Browser"
            st.rerun()
    
    st.markdown("---")
    
    # Quick Start Guide
    with st.expander("📖 Quick Start Guide"):
        st.markdown("""
        #### 1. Preprocess Your Event Log
        - Navigate to the **Preprocess** page
        - Upload your event log file (XES, CSV, or JSON)
        - Configure field mappings to match your data
        - Click **Run Preprocess** to process the log
        
        #### 2. Mine Constraints
        - Go to the **Mining** page
        - Select the preprocessed log
        - Configure constraint types and thresholds
        - Click **Run Mining** to discover constraints
        
        #### 3. Browse Results
        - Visit the **Storage Browser** to view processed logs
        - Download mining results from the output directory
        - Analyze discovered constraints
        """)
    
    # Recent Activity (placeholder)
    st.markdown("### 📊 Recent Activity")
    st.info("📝 No recent activity. Start by preprocessing a log file!")

elif page == "🔄 Preprocess":
    from pages import preprocess
    preprocess.render()

elif page == "⛏️ Mining":
    from pages import mining
    mining.render()

elif page == "💾 Storage Browser":
    from pages import storage_browser
    storage_browser.render()

elif page == "⚙️ Settings":
    from pages import settings
    settings.render()

# Handle navigation override (from buttons)
if 'nav_override' in st.session_state:
    if st.session_state.nav_override != page:
        st.rerun()
    else:
        del st.session_state.nav_override

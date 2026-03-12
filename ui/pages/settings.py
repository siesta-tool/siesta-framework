"""
Settings page.
"""
import streamlit as st
import os


def render():
    """Render the Settings page."""
    st.title("⚙️ Settings")
    st.markdown("Configure connection settings and preferences.")
    
    # API Settings
    st.markdown("### 🔌 API Connection")
    
    with st.form("api_settings"):
        api_url = st.text_input(
            "API Base URL",
            value=st.session_state.api_url,
            help="Base URL for the Siesta Framework API"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.form_submit_button("💾 Save", type="primary", use_container_width=True):
                st.session_state.api_url = api_url
                st.success("✅ API settings saved!")
        
        with col2:
            if st.form_submit_button("🔄 Reset to Default", use_container_width=True):
                st.session_state.api_url = "http://localhost:8000"
                st.rerun()
    
    # Test API connection
    from utils.api_client import SiestaAPIClient
    api_client = SiestaAPIClient(st.session_state.api_url)
    
    if st.button("🧪 Test Connection"):
        with st.spinner("Testing API connection..."):
            if api_client.health_check():
                st.success(f"✅ Successfully connected to API at {st.session_state.api_url}")
            else:
                st.error(f"❌ Failed to connect to API at {st.session_state.api_url}")
                st.info("Make sure the Siesta Framework API is running.")
    
    st.markdown("---")
    
    # S3 Settings
    st.markdown("### 💾 S3 Storage")
    
    with st.form("s3_settings"):
        col1, col2 = st.columns(2)
        
        with col1:
            s3_endpoint = st.text_input(
                "S3 Endpoint",
                value=st.session_state.s3_config['endpoint'],
                help="S3-compatible storage endpoint URL"
            )
            
            s3_access_key = st.text_input(
                "Access Key",
                value=st.session_state.s3_config['access_key'],
                type="password"
            )
        
        with col2:
            s3_region = st.text_input(
                "Region",
                value=st.session_state.s3_config['region'],
                help="S3 region"
            )
            
            s3_secret_key = st.text_input(
                "Secret Key",
                value=st.session_state.s3_config['secret_key'],
                type="password"
            )
        
        default_namespace = st.text_input(
            "Default Namespace (Bucket)",
            value=st.session_state.default_namespace,
            help="Default bucket name for storage operations"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.form_submit_button("💾 Save", type="primary", use_container_width=True):
                st.session_state.s3_config = {
                    'endpoint': s3_endpoint,
                    'access_key': s3_access_key,
                    'secret_key': s3_secret_key,
                    'region': s3_region
                }
                st.session_state.default_namespace = default_namespace
                st.success("✅ S3 settings saved!")
        
        with col2:
            if st.form_submit_button("🔄 Reset to Default", use_container_width=True):
                st.session_state.s3_config = {
                    'endpoint': 'http://localhost:9000',
                    'access_key': 'minioadmin',
                    'secret_key': 'minioadmin',
                    'region': 'us-east-1'
                }
                st.session_state.default_namespace = 'siesta'
                st.rerun()
    
    # Test S3 connection
    from utils.s3_browser import S3Browser
    
    if st.button("🧪 Test S3 Connection"):
        with st.spinner("Testing S3 connection..."):
            s3_browser = S3Browser(
                endpoint=st.session_state.s3_config['endpoint'],
                access_key=st.session_state.s3_config['access_key'],
                secret_key=st.session_state.s3_config['secret_key'],
                region=st.session_state.s3_config['region']
            )
            
            if s3_browser.client:
                buckets = s3_browser.list_buckets()
                st.success(f"✅ Successfully connected to S3")
                st.info(f"Found {len(buckets)} bucket(s): {', '.join(buckets) if buckets else 'none'}")
            else:
                st.error("❌ Failed to connect to S3")
    
    st.markdown("---")
    
    # Application Info
    st.markdown("### ℹ️ Application Info")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Siesta Framework UI**
        - Version: 1.0.0
        - Built with: Streamlit
        - Python: 3.10+
        """)
    
    with col2:
        st.markdown("""
        **Current Configuration**
        - API URL: `{}`
        - S3 Endpoint: `{}`
        - Default Namespace: `{}`
        """.format(
            st.session_state.api_url,
            st.session_state.s3_config['endpoint'],
            st.session_state.default_namespace
        ))
    
    st.markdown("---")
    
    # Environment Variables
    with st.expander("🔧 Environment Variables (.env)"):
        st.markdown("""
        You can configure default settings using environment variables.
        Create a `.env` file in the `ui/` directory with the following:
        
        ```bash
        API_BASE_URL=http://localhost:8000
        S3_ENDPOINT=http://localhost:9000
        S3_ACCESS_KEY=minioadmin
        S3_SECRET_KEY=minioadmin
        S3_REGION=us-east-1
        DEFAULT_NAMESPACE=siesta
        ```
        """)
    
    # Clear cache
    if st.button("🗑️ Clear Cache & Restart"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("✅ Cache cleared!")
        st.info("Please refresh the page to restart the application.")

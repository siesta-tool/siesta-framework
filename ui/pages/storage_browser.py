"""
Storage browser page.
"""
import streamlit as st
import pandas as pd
from utils.s3_browser import S3Browser
from datetime import datetime


@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_s3_buckets(endpoint, access_key, secret_key, region):
    """Get list of S3 buckets with caching."""
    s3_browser = S3Browser(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region
    )
    if not s3_browser.client:
        return None
    return s3_browser.list_buckets()

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_s3_logs(endpoint, access_key, secret_key, region, bucket):
    """Get list of logs in a bucket with caching."""
    s3_browser = S3Browser(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region
    )
    if not s3_browser.client:
        return None
    return s3_browser.list_logs(bucket)

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_s3_tables(endpoint, access_key, secret_key, region, bucket, log_name):
    """Get list of tables in a log with caching."""
    s3_browser = S3Browser(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region
    )
    if not s3_browser.client:
        return None
    return s3_browser.list_tables(bucket, log_name)

def render():
    """Render the Storage Browser page."""
    st.title("💾 Storage Browser")
    st.markdown("Browse and inspect S3 storage contents.")
    
    # Get buckets with caching
    buckets = get_s3_buckets(
        st.session_state.s3_config['endpoint'],
        st.session_state.s3_config['access_key'],
        st.session_state.s3_config['secret_key'],
        st.session_state.s3_config['region']
    )
    
    if buckets is None:
        st.error("❌ Failed to connect to S3 storage. Please check your S3 configuration in Settings.")
        return
    
    if not buckets:
        st.warning("⚠️ No S3 buckets found. Make sure your S3 service is running and accessible.")
        st.info(f"**S3 Endpoint:** {st.session_state.s3_config['endpoint']}")
        return
    
    # Bucket selection
    st.markdown("### 🗂️ Select Storage Namespace")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        default_bucket = st.session_state.default_namespace if st.session_state.default_namespace in buckets else buckets[0]
        selected_bucket = st.selectbox(
            "Bucket",
            buckets,
            index=buckets.index(default_bucket) if default_bucket in buckets else 0,
            label_visibility="collapsed"
        )
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            # Clear cache before refreshing
            get_s3_buckets.clear()
            get_s3_logs.clear()
            get_s3_tables.clear()
            st.rerun()
    
    st.markdown("---")
    
    # Get logs in the selected bucket
    logs = get_s3_logs(
        st.session_state.s3_config['endpoint'],
        st.session_state.s3_config['access_key'],
        st.session_state.s3_config['secret_key'],
        st.session_state.s3_config['region'],
        selected_bucket
    )
    
    if not logs:
        st.info(f"📂 No event logs found in bucket **{selected_bucket}**.")
        st.markdown("""
        ### Getting Started
        
        To create your first event log:
        1. Go to the **Preprocess** page
        2. Upload an event log file
        3. Configure the preprocessing settings
        4. The processed log will appear here
        """)
        return
    
    # Display logs overview
    st.markdown(f"### 📊 Event Logs in `{selected_bucket}`")
    st.caption(f"Found {len(logs)} event log(s)")
    
    # Create a DataFrame for logs overview
    logs_data = []
    for log in logs:
        log_info = {
            'Log Name': log['name'],
            'Last Modified': log['metadata'].get('last_modified', 'Unknown') if log.get('metadata') else 'Unknown',
            'Size (MB)': f"{log['metadata'].get('total_size', 0) / 1024 / 1024:.2f}" if log.get('metadata') else '0.00',
            'Files': log['metadata'].get('file_count', 0) if log.get('metadata') else 0
        }
        logs_data.append(log_info)
    
    logs_df = pd.DataFrame(logs_data)
    
    # Display logs table
    st.dataframe(
        logs_df,
        use_container_width=True,
        hide_index=True
    )
    
    st.markdown("---")
    
    # Log details section
    st.markdown("### 🔍 Inspect Event Log")
    
    log_names = [log['name'] for log in logs]
    selected_log = st.selectbox(
        "Select a log to inspect",
        log_names,
        label_visibility="collapsed"
    )
    
    if selected_log:
        # Get tables for this log
        tables = get_s3_tables(
            st.session_state.s3_config['endpoint'],
            st.session_state.s3_config['access_key'],
            st.session_state.s3_config['secret_key'],
            st.session_state.s3_config['region'],
            selected_bucket,
            selected_log
        )
        
        if not tables:
            st.warning(f"⚠️ No tables found for log **{selected_log}**")
            return
        
        # Display log metadata
        log_obj = next((log for log in logs if log['name'] == selected_log), None)
        if log_obj and log_obj.get('metadata'):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Total Size",
                    f"{log_obj['metadata'].get('total_size', 0) / 1024 / 1024:.2f} MB"
                )
            
            with col2:
                st.metric(
                    "Total Files",
                    log_obj['metadata'].get('file_count', 0)
                )
            
            with col3:
                last_mod = log_obj['metadata'].get('last_modified', 'Unknown')
                if last_mod != 'Unknown':
                    try:
                        dt = datetime.fromisoformat(last_mod.replace('Z', '+00:00'))
                        last_mod = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                st.metric("Last Modified", last_mod)
        
        st.markdown("---")
        
        # Display tables
        st.markdown(f"#### 📋 Tables in `{selected_log}`")
        
        # Create tabs for different table categories
        system_tables = [t for t in tables if t['name'] in ['metadata', 'last_checked', 'trace_metadata']]
        data_tables = [t for t in tables if t['name'] in ['events', 'sequence', 'activity_index']]
        constraint_tables = [t for t in tables if t['name'] in ['pairs_index', 'active_pairs', 'count']]
        other_tables = [t for t in tables if t not in system_tables + data_tables + constraint_tables]
        
        tab1, tab2, tab3, tab4 = st.tabs([
            f"📊 Data Tables ({len(data_tables)})",
            f"🔗 Constraint Tables ({len(constraint_tables)})",
            f"⚙️ System Tables ({len(system_tables)})",
            f"📁 Other ({len(other_tables)})"
        ])
        
        with tab1:
            render_tables_list(data_tables, selected_bucket, selected_log)
        
        with tab2:
            render_tables_list(constraint_tables, selected_bucket, selected_log)
        
        with tab3:
            render_tables_list(system_tables, selected_bucket, selected_log)
        
        with tab4:
            if other_tables:
                render_tables_list(other_tables, selected_bucket, selected_log)
            else:
                st.info("No other tables found.")


def render_tables_list(tables, bucket, log_name):
    """Render a list of tables with details."""
    if not tables:
        st.info("No tables in this category.")
        return
    
    for table in tables:
        with st.expander(f"**{table['name']}**"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Size", f"{table['size'] / 1024:.2f} KB")
            
            with col2:
                st.metric("Files", table['files'])
            
            with col3:
                last_mod = table['last_modified']
                if last_mod:
                    try:
                        dt = datetime.fromisoformat(last_mod.replace('Z', '+00:00'))
                        last_mod_str = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        last_mod_str = last_mod
                    st.metric("Last Modified", last_mod_str)
            
            st.caption(f"**Path:** `s3://{bucket}/{table['path']}`")
            
            # Table description based on name
            descriptions = {
                'events': 'Raw event data from the log',
                'sequence': 'Processed event sequences with positions',
                'activity_index': 'Index of all activities in the log',
                'pairs_index': 'Index of activity pairs for constraint mining',
                'active_pairs': 'Currently active pairs during streaming',
                'count': 'Count statistics for activities and pairs',
                'metadata': 'Log metadata and configuration',
                'last_checked': 'Timestamp tracking for incremental processing',
                'trace_metadata': 'Metadata for individual traces'
            }
            
            if table['name'] in descriptions:
                st.info(f"ℹ️ {descriptions[table['name']]}")


def format_bytes(bytes_val):
    """Format bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} TB"

"""
Preprocess module page.
"""
import streamlit as st
import json
from utils.api_client import SiestaAPIClient


def render():
    """Render the Preprocess page."""
    st.title("🔄 Preprocess Event Logs")
    st.markdown("Transform and load event logs into the Siesta storage system.")
    
    # Create tabs for different modes
    tab1, tab2, tab3 = st.tabs(["📤 Batch Upload", "🌊 Streaming Mode", "📋 Presets"])
    
    with tab1:
        render_batch_mode()
    
    with tab2:
        render_streaming_mode()
    
    with tab3:
        render_presets()


def render_batch_mode():
    """Render batch upload mode."""
    st.markdown("### Upload and Process Event Log")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # File upload
        uploaded_file = st.file_uploader(
            "Choose an event log file",
            type=['xes', 'csv', 'json'],
            help="Upload XES, CSV, or JSON formatted event log"
        )
        
        if uploaded_file:
            st.success(f"✓ File selected: **{uploaded_file.name}** ({uploaded_file.size / 1024:.2f} KB)")
    
    with col2:
        st.markdown("#### Supported Formats")
        st.markdown("""
        - **XES**: IEEE XES standard
        - **CSV**: Comma-separated values
        - **JSON**: JSON Lines or array
        """)
    
    st.markdown("---")
    
    # Configuration form
    with st.form("preprocess_config"):
        st.markdown("### Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            log_name = st.text_input(
                "Log Name",
                value="my_event_log",
                help="Unique identifier for this event log"
            )
            
            namespace = st.text_input(
                "Storage Namespace",
                value=st.session_state.default_namespace,
                help="S3 bucket name for storage"
            )
            
            overwrite = st.checkbox(
                "Overwrite existing data",
                value=False,
                help="Delete existing data for this log before processing"
            )
        
        with col2:
            # File format detection
            file_format = "csv"
            if uploaded_file:
                if uploaded_file.name.endswith('.xes'):
                    file_format = "xes"
                elif uploaded_file.name.endswith('.json'):
                    file_format = "json"
            
            format_option = st.selectbox(
                "File Format",
                ["csv", "xes", "json"],
                index=["csv", "xes", "json"].index(file_format),
                help="Format of the uploaded file"
            )
            
            lookback = st.text_input(
                "Lookback Period",
                value="7d",
                help="For incremental processing (e.g., '7d', '24h')"
            )
        
        st.markdown("#### Field Mappings")
        st.markdown(f"Configure how fields in your **{format_option.upper()}** file map to Siesta's data model.")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            activity_field = st.text_input(
                "Activity Field",
                value="concept:name" if format_option == "xes" else "activity",
                help="Column containing activity names"
            )
            
            trace_id_field = st.text_input(
                "Trace ID Field",
                value="case:concept:name" if format_option == "xes" else "trace_id",
                help="Column containing trace/case identifiers"
            )
        
        with col2:
            timestamp_field = st.text_input(
                "Timestamp Field",
                value="time:timestamp" if format_option == "xes" else "timestamp",
                help="Column containing event timestamps"
            )
            
            position_field = st.text_input(
                "Position Field (optional)",
                value="",
                help="Column containing event position in trace (leave empty for auto)"
            )
        
        with col3:
            attributes_text = st.text_area(
                "Additional Attributes",
                value="*" if format_option == "xes" else "resource, cost",
                help="Comma-separated list of attribute columns to include (use * for all)",
                height=100
            )
        
        st.markdown("---")
        
        # Submit button
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            submitted = st.form_submit_button(
                "▶️ Run Preprocess",
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            if st.form_submit_button("🔄 Reset Form", use_container_width=True):
                st.rerun()
    
    # Process the request
    if submitted:
        if not uploaded_file:
            st.error("❌ Please upload a file before running preprocess.")
            return
        
        # Parse attributes
        if attributes_text.strip() == "*":
            attributes = ["*"]
        else:
            attributes = [attr.strip() for attr in attributes_text.split(",") if attr.strip()]
        
        # Build configuration
        preprocess_config = {
            "log_name": log_name,
            "storage_namespace": namespace,
            "overwrite_data": overwrite,
            "enable_streaming": False,
            "lookback": lookback,
            "field_mappings": {
                format_option: {
                    "activity": activity_field,
                    "trace_id": trace_id_field,
                    "start_timestamp": timestamp_field,
                    "position": position_field if position_field else None,
                    "attributes": attributes
                }
            },
            "trace_level_fields": [trace_id_field],
            "timestamp_fields": [timestamp_field]
        }
        
        # Show configuration preview
        with st.expander("📋 Configuration Preview"):
            st.json(preprocess_config)
        
        # Execute preprocessing
        with st.spinner("⚙️ Processing event log... This may take a few minutes."):
            try:
                api_client = SiestaAPIClient(st.session_state.api_url)
                
                # Read file bytes
                file_bytes = uploaded_file.read()
                
                # Call API
                result = api_client.preprocess_run(
                    preprocess_config=preprocess_config,
                    log_file=file_bytes,
                    filename=uploaded_file.name
                )
                
                if result['success']:
                    st.success("✅ Preprocessing completed successfully!")
                    st.info(f"**Response:** {result['data']}")
                    
                    # Show next steps
                    st.markdown("### 🎉 Next Steps")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("⛏️ Go to Mining", use_container_width=True):
                            st.session_state.nav_override = "⛏️ Mining"
                            st.rerun()
                    with col2:
                        if st.button("💾 View in Storage", use_container_width=True):
                            st.session_state.nav_override = "💾 Storage Browser"
                            st.rerun()
                else:
                    st.error(f"❌ Preprocessing failed: {result.get('error', 'Unknown error')}")
                    if result.get('status_code'):
                        st.error(f"Status Code: {result['status_code']}")
                
            except Exception as e:
                st.error(f"❌ Error during preprocessing: {str(e)}")


def render_streaming_mode():
    """Render streaming mode configuration."""
    st.markdown("### Configure Streaming Collector")
    st.info("📡 Streaming mode collects events from Kafka in real-time.")
    
    with st.form("streaming_config"):
        col1, col2 = st.columns(2)
        
        with col1:
            log_name = st.text_input("Log Name", value="streaming_log")
            namespace = st.text_input("Storage Namespace", value=st.session_state.default_namespace)
            kafka_topic = st.text_input("Kafka Topic", value="example_log")
        
        with col2:
            lookback = st.text_input("Lookback Period", value="7d")
            format_option = st.selectbox("Event Format", ["json", "csv"])
        
        st.markdown("#### Field Mappings")
        
        col1, col2 = st.columns(2)
        with col1:
            activity_field = st.text_input("Activity Field", value="activity")
            trace_id_field = st.text_input("Trace ID Field", value="trace_id")
        
        with col2:
            timestamp_field = st.text_input("Timestamp Field", value="timestamp")
            position_field = st.text_input("Position Field (optional)", value="")
        
        submitted = st.form_submit_button("▶️ Start Streaming Collector", type="primary")
        
        if submitted:
            preprocess_config = {
                "log_name": log_name,
                "storage_namespace": namespace,
                "enable_streaming": True,
                "lookback": lookback,
                "kafka_topic": kafka_topic,
                "field_mappings": {
                    format_option: {
                        "activity": activity_field,
                        "trace_id": trace_id_field,
                        "start_timestamp": timestamp_field,
                        "position": position_field if position_field else None,
                        "attributes": ["*"]
                    }
                },
                "trace_level_fields": [trace_id_field],
                "timestamp_fields": [timestamp_field]
            }
            
            with st.spinner("⚙️ Initializing streaming collector..."):
                try:
                    api_client = SiestaAPIClient(st.session_state.api_url)
                    result = api_client.preprocess_run(preprocess_config=preprocess_config)
                    
                    if result['success']:
                        st.success("✅ Streaming collector initialized!")
                        st.info(f"**Response:** {result['data']}")
                    else:
                        st.error(f"❌ Failed to start streaming: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")


def render_presets():
    """Render configuration presets."""
    st.markdown("### Configuration Presets")
    st.markdown("Load predefined configurations for common event log formats.")
    
    presets = {
        "BPI Challenge 2017": {
            "format": "csv",
            "log_name": "bpic2017",
            "mappings": {
                "activity": "activity",
                "trace_id": "case_id",
                "timestamp": "timestamp",
                "attributes": ["resource", "cost"]
            }
        },
        "IEEE XES Standard": {
            "format": "xes",
            "log_name": "xes_log",
            "mappings": {
                "activity": "concept:name",
                "trace_id": "case:concept:name",
                "timestamp": "time:timestamp",
                "attributes": ["*"]
            }
        },
        "Custom CSV": {
            "format": "csv",
            "log_name": "custom_csv",
            "mappings": {
                "activity": "activity",
                "trace_id": "trace_id",
                "timestamp": "timestamp",
                "attributes": ["resource"]
            }
        }
    }
    
    for preset_name, preset_config in presets.items():
        with st.expander(f"📄 {preset_name}"):
            st.json(preset_config)
            if st.button(f"Load {preset_name}", key=f"load_{preset_name}"):
                st.session_state.preset_loaded = preset_config
                st.success(f"✓ Loaded preset: {preset_name}")
                st.info("Switch to the 'Batch Upload' tab to use this configuration.")

"""
Mining module page.
"""
import streamlit as st
import pandas as pd
from utils.api_client import SiestaAPIClient
from utils.s3_browser import S3Browser


@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_s3_buckets(endpoint, access_key, secret_key, region):
    """Get list of S3 buckets with caching."""
    s3_browser = S3Browser(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region
    )
    return s3_browser.list_buckets()

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_s3_logs(endpoint, access_key, secret_key, region, namespace):
    """Get list of logs in a namespace with caching."""
    s3_browser = S3Browser(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region
    )
    return s3_browser.list_logs(namespace)

def render():
    """Render the Mining page."""
    st.title("⛏️ Constraint Mining")
    st.markdown("Discover declarative constraints from preprocessed event logs.")
    
    # Configuration form
    with st.form("mining_config"):
        st.markdown("### Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Get available logs
            buckets = get_s3_buckets(
                st.session_state.s3_config['endpoint'],
                st.session_state.s3_config['access_key'],
                st.session_state.s3_config['secret_key'],
                st.session_state.s3_config['region']
            )
            if buckets:
                default_bucket = st.session_state.default_namespace if st.session_state.default_namespace in buckets else buckets[0]
                namespace = st.selectbox(
                    "Storage Namespace",
                    buckets,
                    index=buckets.index(default_bucket) if default_bucket in buckets else 0,
                    help="Select the S3 bucket containing your preprocessed logs"
                )
                
                # Get logs in this bucket
                logs = get_s3_logs(
                    st.session_state.s3_config['endpoint'],
                    st.session_state.s3_config['access_key'],
                    st.session_state.s3_config['secret_key'],
                    st.session_state.s3_config['region'],
                    namespace
                )
                
                if logs:
                    log_names = [log['name'] for log in logs]
                    log_name = st.selectbox(
                        "Select Log",
                        log_names,
                        help="Choose a preprocessed event log to mine"
                    )
                    
                    # Display log metadata
                    selected_log = next((log for log in logs if log['name'] == log_name), None)
                    if selected_log and selected_log.get('metadata'):
                        meta = selected_log['metadata']
                        st.caption(f"📊 Last modified: {meta.get('last_modified', 'Unknown')}")
                        st.caption(f"📦 Size: {meta.get('total_size', 0) / 1024 / 1024:.2f} MB")
                else:
                    st.warning("⚠️ No preprocessed logs found in this namespace. Please preprocess a log first.")
                    log_name = st.text_input("Log Name", value="example_log")
            else:
                st.warning("⚠️ No S3 buckets found. Using default namespace.")
                namespace = st.text_input("Storage Namespace", value=st.session_state.default_namespace)
                log_name = st.text_input("Log Name", value="example_log")
        
        with col2:
            output_path = st.text_input(
                "Output Path",
                value=f"output/{log_name}_constraints.csv",
                help="Path where mining results will be saved"
            )
            
            force_recompute = st.checkbox(
                "Force Recompute",
                value=True,
                help="Recompute all constraints even if cached results exist"
            )
        
        st.markdown("---")
        st.markdown("### Constraint Types")
        st.markdown("Select which types of declarative constraints to discover:")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 📋 Basic Patterns")
            existential = st.checkbox("Existential", value=True, help="Existence, Absence, Exactly constraints")
            positional = st.checkbox("Positional", value=True, help="Init, End constraints")
        
        with col2:
            st.markdown("#### 🔗 Relationships")
            ordered = st.checkbox("Ordered", value=True, help="Response, Precedence, ChainResponse, etc.")
            unordered = st.checkbox("Unordered", value=True, help="Co-existence, Not Co-existence")
        
        with col3:
            st.markdown("#### ⛔ Negations")
            negations = st.checkbox("Negations", value=False, help="Not Response, Not Precedence, etc.")
        
        st.markdown("---")
        st.markdown("### Advanced Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            support_threshold = st.slider(
                "Support Threshold",
                min_value=0.0,
                max_value=1.0,
                value=0.8,
                step=0.05,
                help="Minimum support for a constraint to be considered (0.0 - 1.0)"
            )
        
        with col2:
            confidence_threshold = st.slider(
                "Confidence Threshold",
                min_value=0.0,
                max_value=1.0,
                value=0.9,
                step=0.05,
                help="Minimum confidence for a constraint to be considered (0.0 - 1.0)"
            )
        
        # Build constraint types list
        constraint_types = []
        if existential:
            constraint_types.append("existential")
        if positional:
            constraint_types.append("positional")
        if ordered:
            constraint_types.append("ordered")
        if unordered:
            constraint_types.append("unordered")
        if negations:
            constraint_types.append("negations")
        
        st.markdown("---")
        
        # Submit button
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            submitted = st.form_submit_button(
                "▶️ Run Mining",
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            if st.form_submit_button("🔄 Reset", use_container_width=True):
                st.rerun()
    
    # Process mining request
    if submitted:
        if not constraint_types:
            st.error("❌ Please select at least one constraint type.")
            return
        
        # Build mining configuration
        mining_config = {
            "log_name": log_name,
            "storage_namespace": namespace,
            "output_path": output_path,
            "force_recompute": force_recompute,
            "constraint_types": constraint_types,
            "support_threshold": support_threshold,
            "confidence_threshold": confidence_threshold
        }
        
        # Show configuration preview
        with st.expander("📋 Mining Configuration"):
            st.json(mining_config)
        
        # Execute mining
        progress_bar = st.progress(0, text="Initializing mining process...")
        
        try:
            api_client = SiestaAPIClient(st.session_state.api_url)
            
            progress_bar.progress(25, text="⚙️ Sending request to mining module...")
            
            # Call API
            result = api_client.mining_run(mining_config=mining_config)
            
            progress_bar.progress(75, text="📊 Processing mining results...")
            
            if result['success']:
                progress_bar.progress(100, text="✅ Mining completed!")
                st.success("✅ Constraint mining completed successfully!")
                
                # Display results
                st.markdown("### 📊 Mining Results")
                
                try:
                    # Parse results
                    if isinstance(result['data'], list):
                        df = pd.DataFrame(result['data'])
                        
                        if not df.empty:
                            # Display metrics
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Constraints", len(df))
                            with col2:
                                if 'type' in df.columns:
                                    st.metric("Constraint Types", df['type'].nunique())
                            with col3:
                                if 'support' in df.columns:
                                    st.metric("Avg Support", f"{df['support'].mean():.2f}")
                            with col4:
                                if 'confidence' in df.columns:
                                    st.metric("Avg Confidence", f"{df['confidence'].mean():.2f}")
                            
                            st.markdown("---")
                            
                            # Display results table
                            st.markdown("#### Discovered Constraints")
                            
                            # Add filters
                            col1, col2 = st.columns(2)
                            with col1:
                                if 'type' in df.columns:
                                    type_filter = st.multiselect(
                                        "Filter by Type",
                                        options=df['type'].unique().tolist(),
                                        default=df['type'].unique().tolist()
                                    )
                                    df = df[df['type'].isin(type_filter)]
                            
                            with col2:
                                if 'support' in df.columns:
                                    min_support = st.slider(
                                        "Minimum Support",
                                        0.0, 1.0,
                                        float(df['support'].min()),
                                        0.01
                                    )
                                    df = df[df['support'] >= min_support]
                            
                            # Display table
                            st.dataframe(
                                df,
                                use_container_width=True,
                                height=400
                            )
                            
                            # Download button
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Results (CSV)",
                                data=csv,
                                file_name=f"{log_name}_constraints.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        else:
                            st.info("ℹ️ No constraints discovered with the current settings. Try adjusting the thresholds.")
                    else:
                        st.info(f"**Response:** {result['data']}")
                        
                except Exception as e:
                    st.warning(f"⚠️ Could not parse results as DataFrame: {e}")
                    st.json(result['data'])
                
            else:
                progress_bar.empty()
                st.error(f"❌ Mining failed: {result.get('error', 'Unknown error')}")
                if result.get('status_code'):
                    st.error(f"Status Code: {result['status_code']}")
        
        except Exception as e:
            progress_bar.empty()
            st.error(f"❌ Error during mining: {str(e)}")
    
    # Help section
    with st.expander("ℹ️ About Constraint Types"):
        st.markdown("""
        ### Declarative Constraint Types
        
        #### Existential
        - **Existence(A, n)**: Activity A must occur at least n times
        - **Absence(A, n)**: Activity A must not occur more than n times
        - **Exactly(A, n)**: Activity A must occur exactly n times
        
        #### Positional
        - **Init(A)**: Activity A must be the first activity in a trace
        - **End(A)**: Activity A must be the last activity in a trace
        
        #### Ordered (Binary)
        - **Response(A, B)**: If A occurs, B must eventually follow
        - **Precedence(A, B)**: B can only occur if A has occurred before
        - **ChainResponse(A, B)**: If A occurs, B must immediately follow
        - **ChainPrecedence(A, B)**: B can only occur immediately after A
        
        #### Unordered (Binary)
        - **CoExistence(A, B)**: If A occurs, B must also occur (and vice versa)
        - **NotCoExistence(A, B)**: A and B cannot both occur in the same trace
        
        #### Negations
        - **NotResponse(A, B)**: If A occurs, B must not follow
        - **NotPrecedence(A, B)**: B can occur only if A has not occurred before
        """)

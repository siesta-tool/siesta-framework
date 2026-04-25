from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.statistics.start_activities.log import get as start_activities_module
from pm4py.statistics.end_activities.log import get as end_activities_module
from pm4py.objects.conversion.process_tree import converter as pt_converter
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter
from pm4py.objects.bpmn.exporter import exporter as bpmn_exporter
from pm4py.visualization.dfg import visualizer as dfg_visualizer
from pm4py.objects.conversion.dfg import converter as dfg_converter
import tempfile


def _format_duration(seconds: float) -> str:
    """
    Formats duration in seconds to a human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2.5s", "3.2m", "1.5h", "2.1d")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"


def _add_durations_to_petri_net(net, activity_durations: dict):
    """
    Modifies Petri net transition labels to include average durations.

    Args:
        net: Petri net object
        activity_durations: Dict mapping activity names to avg duration in seconds
    """
    if not activity_durations:
        return

    for transition in net.transitions:
        if transition.label and transition.label in activity_durations:
            duration = activity_durations[transition.label]
            transition.label = f"{transition.label}\n({_format_duration(duration)})"


def _add_durations_to_bpmn(bpmn_model, activity_durations: dict):
    """
    Modifies BPMN task labels to include average durations.

    Args:
        bpmn_model: BPMN model object
        activity_durations: Dict mapping activity names to avg duration in seconds
    """
    if not activity_durations:
        return

    # Access BPMN nodes and update task names
    try:
        for node in bpmn_model.get_nodes():
            if hasattr(node, 'get_name') and hasattr(node, 'set_name'):
                name = node.get_name()
                if name and name in activity_durations:
                    duration = activity_durations[name]
                    node.set_name(f"{name}\n({_format_duration(duration)})")
    except Exception:
        # If BPMN structure doesn't match expected, skip duration annotation
        pass


def discover_process_model(df: SparkDataFrame, algo: str = "inductive", output_type: str = "petrinet",
                           noise_threshold: float = 0.0, filter_percentile: float = 0.0,
                           activity_durations: dict = None):
    """
    Discovers a process model from an event log Spark DataFrame using PM4Py algorithms.

    Args:
        df: pyspark.sql.DataFrame with columns 'case:concept:name', 'concept:name',
            'time:timestamp' (or the legacy names 'trace_id', 'event_type', 'timestamp'
            which will be renamed automatically).
        algo: Discovery algorithm to use ('inductive', 'alpha', 'heuristics', 'dfg')
        output_type: Output format ('petrinet' or 'bpmn')
        noise_threshold: Noise filtering threshold (0.0-1.0). Higher values = simpler models.
                        - For inductive: 0.2-0.4 recommended for noisy logs
                        - For heuristics: used as dependency_threshold
                        - Ignored for alpha (doesn't support filtering)
        filter_percentile: Pre-filter log variants by frequency percentile (0.0-1.0).
                          E.g., 0.2 = keep only variants that represent top 80% of cases.
                          0.0 = no filtering.
        activity_durations: Optional dict mapping activity names to average duration in seconds.
                           If provided, durations will be displayed in visualizations.

    Returns:
        tuple: (file_path, format_extension, visualization_path)
            - file_path: Path to the exported model file (PNML for Petri nets, BPMN for BPMN)
            - format_extension: File extension for the model format
            - visualization_path: Path to the visualization PNG file

    Raises:
        ValueError: If invalid algorithm or output type specified
    """

    # Validate parameters
    valid_algos = ['inductive', 'alpha', 'heuristics', 'dfg']
    valid_types = ['petrinet', 'bpmn']

    if algo not in valid_algos:
        raise ValueError(f"Invalid algorithm '{algo}'. Must be one of: {valid_algos}")

    if output_type not in valid_types:
        raise ValueError(f"Invalid output type '{output_type}'. Must be one of: {valid_types}")

    # --- Spark pre-processing ---------------------------------------------
    # Spark DataFrames are immutable, so there is no equivalent of df.copy();
    # every transformation returns a new DataFrame.

    # Ensure proper column names for PM4Py (Spark uses withColumnRenamed,
    # which returns a new DF rather than mutating in place like pandas rename).
    if 'trace_id' in df.columns:
        df = df.withColumnRenamed('trace_id', 'case:concept:name')
    if 'event_type' in df.columns:
        df = df.withColumnRenamed('event_type', 'concept:name')
    if 'timestamp' in df.columns:
        df = df.withColumnRenamed('timestamp', 'time:timestamp')

    # Convert timestamp to TimestampType if it isn't already.
    # pd.to_datetime -> F.to_timestamp. We only cast if the current dtype
    # is not already a timestamp, to avoid needless work.
    if 'time:timestamp' in df.columns:
        ts_dtype = dict(df.dtypes).get('time:timestamp', '')
        if ts_dtype != 'timestamp':
            df = df.withColumn('time:timestamp',
                               F.to_timestamp(F.col('time:timestamp')))

    # Sort by case and timestamp. pandas sort_values([...]) -> Spark orderBy([...]).
    # Note: orderBy triggers a global sort (shuffle); PM4Py also expects the
    # events to be in chronological order per case, so this is required.
    df = df.orderBy(['case:concept:name', 'time:timestamp'])

    # --- Hand off to PM4Py -------------------------------------------------
    # PM4Py cannot consume a Spark DataFrame directly, so we collect to the
    # driver as a pandas DataFrame here. This is the memory boundary.
    pandas_df = df.toPandas()

    # Convert to PM4Py event log format
    pandas_df = dataframe_utils.convert_timestamp_columns_in_df(pandas_df)
    event_log = log_converter.apply(pandas_df)

    # Apply variant filtering if requested
    if filter_percentile > 0.0:
        from pm4py.algo.filtering.log.variants import variants_filter
        event_log = variants_filter.filter_log_variants_percentage(event_log, filter_percentile)

    # Discover model based on algorithm
    if algo == 'dfg':
        # DFG is handled differently - no process tree
        return _discover_dfg(event_log, output_type, noise_threshold, activity_durations)

    # For other algorithms, get process tree first
    if algo == 'inductive':
        # Apply noise filtering for inductive miner
        if noise_threshold > 0.0:
            from pm4py.algo.discovery.inductive.variants.imf import IMFParameters
            process_tree = inductive_miner.apply(event_log, variant=inductive_miner.Variants.IMf,
                                                 parameters={IMFParameters.NOISE_THRESHOLD: noise_threshold})
        else:
            process_tree = inductive_miner.apply(event_log)
    elif algo == 'alpha':
        # Alpha miner returns Petri net directly, convert to process tree representation
        alpha_result = alpha_miner.apply(event_log)
        # Handle different return formats
        if len(alpha_result) == 4:
            net, initial_marking, final_marking, _ = alpha_result
        else:
            net, initial_marking, final_marking = alpha_result
        return _export_petri_or_bpmn(net, initial_marking, final_marking, output_type, is_from_alpha=True, activity_durations=activity_durations)
    elif algo == 'heuristics':
        # Heuristics miner returns Petri net directly
        # Apply dependency threshold for noise filtering
        if noise_threshold > 0.0:
            from pm4py.algo.discovery.heuristics.variants.classic import Parameters as HeuParams
            parameters = {
                HeuParams.DEPENDENCY_THRESH: noise_threshold,
                HeuParams.AND_MEASURE_THRESH: max(0.65, noise_threshold),
                HeuParams.LOOP_LENGTH_TWO_THRESH: max(0.5, noise_threshold)
            }
            heu_result = heuristics_miner.apply(event_log, parameters=parameters)
        else:
            heu_result = heuristics_miner.apply(event_log)
        # Handle different return formats
        if len(heu_result) == 4:
            net, initial_marking, final_marking, _ = heu_result
        else:
            net, initial_marking, final_marking = heu_result
        return _export_petri_or_bpmn(net, initial_marking, final_marking, output_type, is_from_alpha=True, activity_durations=activity_durations)

    # Convert process tree to desired format
    if output_type == 'petrinet':
        net, initial_marking, final_marking = pt_converter.apply(process_tree)
        return _export_petri_or_bpmn(net, initial_marking, final_marking, 'petrinet', activity_durations=activity_durations)
    elif output_type == 'bpmn':
        bpmn_model = pt_converter.apply(process_tree, variant=pt_converter.Variants.TO_BPMN)
        return _export_bpmn_model(bpmn_model, activity_durations=activity_durations)


def _discover_dfg(event_log, output_type: str, noise_threshold: float = 0.0, activity_durations: dict = None):
    """
    Discovers a Directly-Follows Graph from the event log.

    Args:
        event_log: PM4Py event log
        output_type: Output format ('petrinet' or 'bpmn')
        noise_threshold: If > 0, filters DFG edges by frequency percentile
        activity_durations: Optional dict mapping activity names to average duration in seconds

    Returns:
        tuple: (file_path, format_extension, visualization_path)
    """
    # Discover DFG
    dfg = dfg_discovery.apply(event_log)
    start_activities = start_activities_module.get_start_activities(event_log)
    end_activities = end_activities_module.get_end_activities(event_log)

    # Apply DFG filtering based on noise threshold
    if noise_threshold > 0.0:
        from pm4py.algo.filtering.dfg import dfg_filtering
        # Filter by percentile - noise_threshold represents the percentage to filter out
        dfg, start_activities, end_activities = dfg_filtering.filter_dfg_on_paths_percentage(
            dfg, start_activities, end_activities, 1.0 - noise_threshold
        )

    # Create temporary files
    vis_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png', mode='wb')
    vis_path = vis_file.name
    vis_file.close()

    if output_type == 'petrinet':
        # Convert DFG to Petri net using the appropriate variant
        from pm4py.objects.conversion.dfg.variants import to_petri_net_activity_defines_place
        conversion_result = to_petri_net_activity_defines_place.apply(dfg, parameters={
            to_petri_net_activity_defines_place.Parameters.START_ACTIVITIES: start_activities,
            to_petri_net_activity_defines_place.Parameters.END_ACTIVITIES: end_activities
        })

        # Handle different return formats (PM4Py versions may vary)
        if len(conversion_result) == 4:
            net, initial_marking, final_marking, _ = conversion_result
        else:
            net, initial_marking, final_marking = conversion_result

        # Export Petri net (without duration labels for XML format)
        model_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pnml', mode='wb')
        model_path = model_file.name
        model_file.close()

        pnml_exporter.apply(net, initial_marking, model_path, final_marking=final_marking)

        # Add durations to labels for visualization only
        _add_durations_to_petri_net(net, activity_durations)

        # Visualize (handle Graphviz not being available)
        try:
            gviz = pn_visualizer.apply(net, initial_marking, final_marking)
            pn_visualizer.save(gviz, vis_path)
        except Exception:
            # If visualization fails, just leave the vis file empty
            pass

        return model_path, 'pnml', vis_path

    elif output_type == 'bpmn':
        # Convert DFG to BPMN via Petri net
        from pm4py.objects.conversion.dfg.variants import to_petri_net_activity_defines_place
        conversion_result = to_petri_net_activity_defines_place.apply(dfg, parameters={
            to_petri_net_activity_defines_place.Parameters.START_ACTIVITIES: start_activities,
            to_petri_net_activity_defines_place.Parameters.END_ACTIVITIES: end_activities
        })

        # Handle different return formats (PM4Py versions may vary)
        if len(conversion_result) == 4:
            net, initial_marking, final_marking, _ = conversion_result
        else:
            net, initial_marking, final_marking = conversion_result

        # Export Petri net (without duration labels for XML format)
        model_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pnml', mode='wb')
        model_path = model_file.name
        model_file.close()

        pnml_exporter.apply(net, initial_marking, model_path, final_marking=final_marking)

        # Add durations to labels for visualization only
        _add_durations_to_petri_net(net, activity_durations)

        # Visualize Petri net (handle Graphviz not being available)
        try:
            gviz = pn_visualizer.apply(net, initial_marking, final_marking)
            pn_visualizer.save(gviz, vis_path)
        except Exception:
            # If visualization fails, just leave the vis file empty
            pass

        return model_path, 'pnml', vis_path


def _export_petri_or_bpmn(net, initial_marking, final_marking, output_type: str, is_from_alpha: bool = False, activity_durations: dict = None):
    """
    Exports a Petri net to the specified format (Petri net PNML or BPMN).

    Args:
        net: Petri net object
        initial_marking: Initial marking
        final_marking: Final marking
        output_type: Output format ('petrinet' or 'bpmn')
        is_from_alpha: Whether the net comes from Alpha/Heuristics miner
        activity_durations: Optional dict mapping activity names to avg duration in seconds

    Returns:
        tuple: (file_path, format_extension, visualization_path)
    """
    # Create temporary files
    vis_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png', mode='wb')
    vis_path = vis_file.name
    vis_file.close()

    if output_type == 'petrinet':
        # Export as PNML (without duration labels for XML format)
        model_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pnml', mode='wb')
        model_path = model_file.name
        model_file.close()

        pnml_exporter.apply(net, initial_marking, model_path, final_marking=final_marking)

        # Add durations to labels for visualization only
        _add_durations_to_petri_net(net, activity_durations)

        # Visualize Petri net (handle Graphviz not being available)
        try:
            gviz = pn_visualizer.apply(net, initial_marking, final_marking)
            pn_visualizer.save(gviz, vis_path)
        except Exception:
            # If visualization fails, just leave the vis file empty
            pass

        return model_path, 'pnml', vis_path

    elif output_type == 'bpmn':
        # For nets from Alpha/Heuristics, we can't easily convert to BPMN
        # Return Petri net instead with a note
        if is_from_alpha:
            model_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pnml', mode='wb')
            model_path = model_file.name
            model_file.close()

            pnml_exporter.apply(net, initial_marking, model_path, final_marking=final_marking)

            # Add durations to labels for visualization only
            _add_durations_to_petri_net(net, activity_durations)

            # Visualize Petri net (handle Graphviz not being available)
            try:
                gviz = pn_visualizer.apply(net, initial_marking, final_marking)
                pn_visualizer.save(gviz, vis_path)
            except Exception:
                # If visualization fails, just leave the vis file empty
                pass

            return model_path, 'pnml', vis_path
        else:
            # This shouldn't happen in our flow but handle it gracefully
            model_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pnml', mode='wb')
            model_path = model_file.name
            model_file.close()

            pnml_exporter.apply(net, initial_marking, model_path, final_marking=final_marking)

            # Add durations to labels for visualization only
            _add_durations_to_petri_net(net, activity_durations)

            # Visualize (handle Graphviz not being available)
            try:
                gviz = pn_visualizer.apply(net, initial_marking, final_marking)
                pn_visualizer.save(gviz, vis_path)
            except Exception:
                # If visualization fails, just leave the vis file empty
                pass

            return model_path, 'pnml', vis_path


def _export_bpmn_model(bpmn_model, activity_durations: dict = None):
    """
    Exports a BPMN model.

    Args:
        bpmn_model: BPMN model object
        activity_durations: Optional dict mapping activity names to avg duration in seconds

    Returns:
        tuple: (file_path, format_extension, visualization_path)
    """
    # Create temporary files
    model_file = tempfile.NamedTemporaryFile(delete=False, suffix='.bpmn', mode='wb')
    model_path = model_file.name
    model_file.close()

    vis_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png', mode='wb')
    vis_path = vis_file.name
    vis_file.close()

    # Export BPMN (without duration labels for XML format)
    bpmn_exporter.apply(bpmn_model, model_path)

    # Add durations to labels for visualization only
    _add_durations_to_bpmn(bpmn_model, activity_durations)

    # Visualize BPMN (handle Graphviz not being available)
    try:
        gviz = bpmn_visualizer.apply(bpmn_model)
        bpmn_visualizer.save(gviz, vis_path)
    except Exception:
        # If visualization fails, just leave the vis file empty
        pass

    return model_path, 'bpmn', vis_path
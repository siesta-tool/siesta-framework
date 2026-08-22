from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
import logging

from siesta.core.sparkManager import get_spark_session
from siesta.model.StorageModel import MetaData

logger = logging.getLogger(__name__)

# Explicit schema for the mined-rules CSV – avoids inferSchema overhead on
# large files and guarantees consistent column types.
_RULES_SCHEMA = StructType(
    [
        StructField("category", StringType(), nullable=True),
        StructField("template", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
        StructField("target", StringType(), nullable=True),
        StructField("occurrences", DoubleType(), nullable=True),
        StructField("support", DoubleType(), nullable=True),
    ]
)


def load_response_rules(config, metadata: MetaData) -> DataFrame:
    """
    Read a mined-rules CSV and return a Spark DataFrame of de-duplicated
    ``response`` rules that meet the minimum support threshold.

    Parameters
    ----------
    csv_path : str
        Path to the mined-rules CSV file (local or ``s3a://`` URI).
    min_support : float, optional
        Per-edge support floor.  Only rules with support ≥ this value are
        retained.  Default: ``0.5``.

    Returns
    -------
    DataFrame
        Spark DataFrame with columns ``[source: string, target: string,
        support: double]``.
    """
    spark = get_spark_session()

    csv_path = config["traces_path"]
    min_support = config["prune_support"]

    logger.info(f"Loading mined rules from {csv_path} with min_support={min_support}...")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(_RULES_SCHEMA)
        .load(csv_path)
    )

    logger.info(f"Total rules loaded: {df.count()}")

    filtered = df.filter(
        (F.col("category") == "ordered")
        & (F.col("template") == "response")
        & (F.col("support") >= min_support)
    )

    # filtered.show(10, truncate=False)

    print(filtered.count())

    # Producing chains of rules (A -> B, B -> C -> A -> C).

    stage1 = filtered.join(
        filtered.select(F.col("source").alias("intermediate"), F.col("target").alias("final_target"), F.col("support").alias("target_support")),
        filtered.target == F.col("intermediate"),
        how="inner",
    ).select(
        F.col("source"),
        F.col("intermediate"),
        F.col("final_target").alias("target"),
        F.col("support"),
        F.col("target_support").alias("R1_support"),
        (F.col("support") * F.col("target_support")).alias("cum_support"),
    ).distinct().filter(F.col("cum_support") >= min_support)

    stage1.show(10, truncate=False)
    print(f"Filtered rules round 1 count: {stage1.count()}")


    # Stage 2: Join stage1 with filtered again to find chains of length 3 (A -> B -> C -> D).
    stage2 = stage1.join(
        filtered.select(F.col("source").alias("intermediate2"), F.col("target").alias("final_target2"), F.col("support").alias("target2_support")),
        stage1.target == F.col("intermediate2"),
        how="inner",
    ).select(
        stage1.source,
        stage1.intermediate,
        stage1.target.alias("intermediate2"),
        F.col("final_target2").alias("target"),
        stage1.support.alias("R1_support"),
        F.col("target2_support").alias("R2_support"),
        (stage1.support * F.col("target2_support")).alias("cum_support"),
    ).distinct().filter((F.col("cum_support") >= min_support) & (F.col("target") != stage1.source))

    print(f"Filtered rules round 2 count: {stage2.count()}")

    # Stage 3: Join stage2 with filtered again to find chains of length 4 (A -> B -> C -> D -> E).
    stage3 = stage2.join(
        filtered.select(F.col("source").alias("intermediate3"), F.col("target").alias("final_target3"), F.col("support").alias("target3_support")),
        stage2.target == F.col("intermediate3"),
        how="inner",
    ).select(
        stage2.source,
        stage2.intermediate,
        stage2.intermediate2,
        stage2.target.alias("intermediate3"),
        F.col("final_target3").alias("target"),
        stage2.R1_support,
        stage2.R2_support,
        F.col("target3_support").alias("R3_support"),
        (stage2.cum_support * F.col("target3_support")).alias("cum_support"),
    ).distinct().filter((F.col("cum_support") >= min_support) & (F.col("target") != stage2.source))

    stage3.show(10, truncate=False)
    print(f"Filtered rules round 3 count: {stage3.count()}")

    # Stage 4: Join stage3 with filtered again to find chains of length 5 (A -> B -> C -> D -> E -> F).
    stage4 = stage3.join(
        filtered.select(F.col("source").alias("intermediate4"), F.col("target").alias("final_target4"), F.col("support").alias("target4_support")),
        stage3.target == F.col("intermediate4"),
        how="inner",
    ).select(
        stage3.source,
        stage3.intermediate,
        stage3.intermediate2,
        stage3.intermediate3,
        stage3.target.alias("intermediate4"),
        F.col("final_target4").alias("target"),
        stage3.R1_support,
        stage3.R2_support,
        stage3.R3_support,
        F.col("target4_support").alias("R4_support"),
        (stage3.cum_support * F.col("target4_support")).alias("cum_support"),
    ).distinct().filter((F.col("cum_support") >= min_support) & (F.col("target") != stage3.source))


    print(f"Filtered rules round 4 count: {stage4.count()}")

    # Stage 5: Join stage4 with filtered again to find chains of length 6 (A -> B -> C -> D -> E -> F -> G).
    stage5 = stage4.join(
        filtered.select(F.col("source").alias("intermediate5"), F.col("target").alias("final_target5"), F.col("support").alias("target5_support")),
        stage4.target == F.col("intermediate5"),
        how="inner",
    ).select(
        stage4.source,
        stage4.intermediate,
        stage4.intermediate2,
        stage4.intermediate3,
        stage4.intermediate4,
        stage4.target.alias("intermediate5"),
        F.col("final_target5").alias("target"),
        stage4.R1_support,
        stage4.R2_support,
        stage4.R3_support,
        stage4.R4_support,
        F.col("target5_support").alias("R5_support"),
        (stage4.cum_support * F.col("target5_support")).alias("cum_support"),
    ).distinct().filter((F.col("cum_support") >= min_support) & (F.col("target") != stage4.source))

    print(f"Filtered rules round 5 count: {stage5.count()}")


    # Stage 6: Join stage5 with filtered again to find chains of length 7 (A -> B -> C -> D -> E -> F -> G -> H).
    stage6 = stage5.join(
        filtered.select(F.col("source").alias("intermediate6"), F.col("target").alias("final_target6"), F.col("support").alias("target6_support")),
        stage5.target == F.col("intermediate6"),
        how="inner",
    ).select(
        stage5.source,
        stage5.intermediate,
        stage5.intermediate2,
        stage5.intermediate3,
        stage5.intermediate4,
        stage5.intermediate5,
        stage5.target.alias("intermediate6"),
        F.col("final_target6").alias("target"),
        stage5.R1_support,
        stage5.R2_support,
        stage5.R3_support,
        stage5.R4_support,
        stage5.R5_support,
        F.col("target6_support").alias("R6_support"),
        (stage5.cum_support * F.col("target6_support")).alias("cum_support"),
    ).distinct().filter((F.col("cum_support") >= min_support) & (F.col("target") != stage5.source))

    print(f"Filtered rules round 6 count: {stage6.count()}")

    return filtered

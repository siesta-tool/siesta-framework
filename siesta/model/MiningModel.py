
class Constraint:
    category: str | None
    template: str | None

    sources: list[str]
    targets: list[str] | None

    trace_ids: list[str]

    occurrences: int | None

    support: float
    confidence: float

    def __init__(self):
        self.category = None
        self.template = None
        self.sources = []
        self.targets = []
        self.occurrences = None
        self.support = 0.0
        self.confidence = 0.0
        self.trace_ids = []
    
    @property   # For existence constraints, since they only have one source, we can refer to it as the activity
    def activity(self):
        return self.sources[0] if self.sources else None
    
    @property
    def is_branched(self):
        return len(self.sources) > 1 or (self.targets and len(self.targets) > 1)
    
    @staticmethod
    def get_schema():
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType
        return StructType([
            StructField("category", StringType(), False),
            StructField("template", StringType(), False),
            StructField("sources", ArrayType(StringType()), False),
            StructField("targets", ArrayType(StringType()), True),
            StructField("occurrences", IntegerType(), True),
            StructField("support", FloatType(), True),
            StructField("confidence", FloatType(), True),
            StructField("trace_ids", ArrayType(StringType()), False)
        ])

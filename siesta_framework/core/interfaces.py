from typing import Any, Callable, Dict, ClassVar, Literal, Tuple
from abc import ABC, abstractmethod
from pyspark import RDD

class SiestaModule(ABC):
    """All Siesta modules must implement to be accessed by the framework.
       eg. ::
        class PreprocessorModule(SiestaModule):
            name = "preprocessor"

            def register_routes(self):
                return {
                    "preprocess": self.preprocess_endpoint
                }

            def startup(self):
                # Initialization code here
                pass
            ...
    """
    
    name: ClassVar[str] = "Unnamed Module"
    version: ClassVar[str] = "unversioned"

    type ApiMethod = Literal["GET", "POST", "PUT", "DELETE"]
    type ApiRoute = Tuple[ApiMethod, Callable] | Tuple[ApiMethod, Callable, Dict[str, Any]]
    type ApiRoutes = Dict[str, ApiRoute]

    # @abstractmethod
    def register_routes(self) -> ApiRoutes | None:
        """Return a dict of endpoint_name -> function."""
        pass

    def startup(self) -> None:
        """Lifecycle hook: Called when the framework starts."""
        pass

    def run(*args: Any, **kwargs: Any) -> Any:
        """Main execution method for the module."""
        pass


class StorageManager(ABC):

    name: ClassVar[str] = "Unnamed Storage Manager"
    version: ClassVar[str] = "unversioned"

    """
    This abstract class defines the interface that each database connector should implement.
    
    There are 3 types of methods:
    - write and read methods: should be implemented by each database, as they might have different
      indexing or structure
    - combine methods: describe how the data already stored in the database are combined with newly
      calculated data
    - spark related methods: initialization, building appropriate tables, and termination
    
    The tables that are utilized are:
    - IndexTable: contains the inverted index based on event type pairs
    - SequenceTable: contains the traces that are indexed
    - SingleTable: contains the single inverted index (similar to Set-Containment)
    - LastCheckedTable: contains the timestamp of the last completion of each event type per trace
    - CountTable: contains basic statistics (max duration, number of completions) for each event type pair
    - MetadataTable: contains information for each log database (compression algorithm, number of traces, etc.)
    """
    
    @abstractmethod
    def initialize_spark(self, config: Dict[str, Any]) -> None:
        """
        Initialize the Spark context depending on the database requirements.
        
        Args:
            config: Configuration dictionary containing Spark and database settings
        """
        pass
    
    @abstractmethod
    def initialize_db(self, config: Dict[str, Any]) -> None:
        """
        Create the appropriate tables and remove previous ones if necessary.
        
        Args:
            config: Configuration dictionary containing database settings
        """
        pass
    
    @abstractmethod
    def get_metadata(self, config: Dict[str, Any]) -> Any:
        """
        Construct metadata based on data already stored in the database and new configuration.
        
        Args:
            config: Configuration dictionary passed during execution
            
        Returns:
            MetaData object containing the metadata
        """
        pass
    
    @abstractmethod
    def write_metadata(self, metadata: Any) -> None:
        """
        Persist metadata to the database.
        
        Args:
            metadata: MetaData object containing the metadata
        """
        pass
    
    
    @abstractmethod
    def read_sequence_table(self, metadata: Any, detailed: bool = False) -> RDD:
        """
        Read data as an RDD from the SequenceTable.
        
        Args:
            metadata: MetaData object containing the metadata
            detailed: Whether to include detailed information
            
        Returns:
            RDD containing EventTrait objects
        """
        pass
    
    @abstractmethod
    def read_single_table(self, metadata: Any) -> RDD:
        """
        Load the single inverted index from the database (stored in SingleTable).
        
        Args:
            metadata: MetaData object containing the metadata
            
        Returns:
            RDD containing Event objects
        """
        pass
    
    @abstractmethod
    def read_last_checked_table(self, metadata: Any) -> RDD:
        """
        Load data from the LastCheckedTable, containing the last timestamp per event type pair per trace.
        
        Args:
            metadata: MetaData object containing the metadata
            
        Returns:
            RDD with last timestamps per event type pair per trace
        """
        pass
    
    @abstractmethod
    def write_last_checked_table(self, last_checked: RDD, metadata: Any) -> None:
        """
        Store new records for last checked timestamps back in the database.
        
        Args:
            last_checked: RDD containing timestamp of last completion for each event type pair per trace
            metadata: MetaData object containing the metadata
        """
        pass
    
    @abstractmethod
    def write_count_table(self, counts: RDD, metadata: Any) -> None:
        """
        Write count statistics to the CountTable.
        
        Args:
            counts: RDD containing calculated basic statistics per event type pair
            metadata: MetaData object containing the metadata
        """
        pass
    
    @abstractmethod
    def write_sequence_table(self, sequence_rdd: RDD, metadata: Any, detailed: bool = False) -> None:
        """
        Write traces to the SequenceTable. The RDD should already be persisted and should not be modified.
        Updates the metadata object.
        
        Args:
            sequence_rdd: RDD containing traces with new events
            metadata: MetaData object containing the metadata
            detailed: Whether to include detailed information
        """
        pass
    
    @abstractmethod
    def write_single_table(self, sequence_rdd: RDD, metadata: Any) -> None:
        """
        Write traces to the SingleTable. The RDD is not persisted and should be persisted by the database
        before storing and unpersisted at the end. Updates the metadata object.
        
        Args:
            sequence_rdd: RDD containing newly indexed events in single inverted index form
            metadata: MetaData object containing the metadata
        """
        pass
    
    @abstractmethod
    def write_index_table(self, new_pairs: RDD, metadata: Any) -> None:
        """
        Write the combined pairs back to the database, grouped by interval and first event.
        
        Args:
            new_pairs: RDD containing newly generated pairs
            metadata: MetaData object containing the metadata
        """
        pass


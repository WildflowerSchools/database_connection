from . import DatabaseConnection
from . import DataQueue

class DatabaseConnectionMemory(DatabaseConnection):
    """
    Class to define a DatabaseConnection to a database in memory
    """
    def __init__(
        self,
        timestamp_field_name = None,
        object_id_field_name = None,
        other_field_names = None):
        """
        Constructor for DatabaseConnectionMemory.

        If timestamp field name is specified, then database is assumed to be a
        time series database, every datapoint written to the database must
        contain a field with this name, and this will enable various
        time-related methods to access the data (e.g., fetching a time span,
        creating an iterator that returns data points in time order).

        If object ID field name is specified, then database is assumed to be
        contain data associated with objects (e.g., measurement devices), every
        datapoint written to the database must contain a field with this name,
        and this will enable various object methods to access the data (e.g.,
        fetching all data associated with a specific list of object IDs).

        The final argument contains the names of the remaining fields. Any
        subsequent data sent to the database that is not associated with one of
        the field names in these three arguments will be silently rejected.

        Parameters:
            timestamp_field_name (string): Name of the field containing the timestamp for each datapoint
            object_id_field_name (string): Name of the field containing the object ID for each datapoint
            other_field_names (list of string): Names of the remaining fields
        """
        self.timestamp_field_name = timestamp_field_name
        self.object_id_field_name = object_id_field_name
        self.other_field_names = other_field_names
        self.field_names = []
        if timestamp_field_name is not None:
            self.field_names += [timestamp_field_name]
        if object_id_field_name is not None:
            self.field_names += [object_id_field_name]
        if other_field_names is not None:
            self.field_names += other_field_names
        self.data = []

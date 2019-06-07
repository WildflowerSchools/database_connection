class DatabaseConnection:
    """
    Class to define a simple, generic database interface that can be adapted to
    many different use cases and implementations.

    All methods must be implemented by derived classes.
    """
    def __init__(
        self,
        timestamp_field_name = None,
        object_id_field_name = None):
        """
        Constructor for DatabaseConnection.

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

        Parameters:
            timestamp_field_name (string): Name of the field containing the timestamp for each datapoint
            object_id_field_name (string): Name of the field containing the object ID for each datapoint
        """
        raise NotImplementedError('Method must be implemented by derived class')

    def create_datapoint(
        self,
        data
    ):
        """
        Write a datapoint to the database.

        The data must be in the form of a dictionary with field names as keys
        and data values as values.

        To accomodate the widest range of implementations, the data values must
        be serializable by both the standard JSON encoder and by their __str__
        format (i.e., essentially built-in Python scalar types). Timestamp
        values (if present) must be given as ISO-format strings. Lists are not
        allowed (to accommodate simple implementation as a tabular database).

        Parameters:
            data (dict): Data to write to the database
        """
        raise NotImplementedError('Method must be implemented by derived class')

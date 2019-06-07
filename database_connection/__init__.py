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

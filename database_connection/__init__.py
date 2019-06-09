class DatabaseConnection:
    """
    Class to define a simple, generic database interface that can be adapted to
    many different use cases and implementations.

    All methods must be implemented by derived classes.
    """
    def __init__(
        self,
        timestamp_field_name = None,
        object_id_field_name = None,
        other_field_names = None):
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

        The final argument contains the names of the remaining fields. Any
        subsequent data sent to the database that is not associated with one of
        the field names in these three arguments will be silently rejected.

        Parameters:
            timestamp_field_name (string): Name of the field containing the timestamp for each datapoint
            object_id_field_name (string): Name of the field containing the object ID for each datapoint
            other_field_names (list of string): Names of the remaining fields
        """
        raise NotImplementedError('Method must be implemented by derived class')

    def write_data(
        self,
        data
    ):
        """
        Write data to the database.

        The data must be in the form of a dictionary with field names as keys
        and data values as values (for a single data point) or a simple list of
        such objects (for multiple datapoints).

        To accomodate the widest range of implementations, the data values must
        be serializable/deserializable by the standard JSON interface. Any other
        type/format conversion must be implemented by the derived class.
        Timestamp values (if present) must be given as ISO-format strings. Lists
        are not allowed as data values (to accommodate simple implementation as
        a tabular database).

        Parameters:
            data (dict or list of dict): Data to write to the database
        """
        raise NotImplementedError('Method must be implemented by derived class')

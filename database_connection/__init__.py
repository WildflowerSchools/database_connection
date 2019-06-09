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

    def fetch_data(
        self,
        start_time = None,
        end_time = None,
        object_ids = None,
        fields = None
    ):
        """
        Fetch data from the database.

        Start time and end time must be ISO-format strings. If start time or end
        time is specified and database does not have a designated timestamp
        field, an exception will be generated.

        If object IDs are specified and database does not have a designated
        object ID field, an exception will be generated.

        If fields are not specified, all fields are returned.

        Parameters:
            start_time (string): Return data with timestamps greater than or equal to this value
            end_time (string): Return data with timestamps less than or equal to this value
            object_ids (list): Return data for these object IDs
            fields (list): Return data for these fields

        Returns:
            (list of dict): Datapoints from database which satisfy the criteria
        """
        raise NotImplementedError('Method must be implemented by derived class')

    def to_data_queue(
        self,
        start_time = None,
        end_time = None,
        object_ids = None,
        fields = None
    ):
        """
        Create an iterable which returns datapoints from the database in time order.

        Start time and end time must be ISO-format strings. If start time or end
        time is specified and database does not have a designated timestamp
        field, an exception will be generated.

        If object IDs are specified and database does not have a designated
        object ID field, an exception will be generated.

        If fields are not specified, all fields are returned.

        If database does not have a designated timestamp field, an exception
        will be generated.

        Parameters:
            start_time (string): Return data with timestamps greater than or equal to this value
            end_time (string): Return data with timestamps less than or equal to this value
            object_ids (list): Return data for these object IDs
            fields (list): Return data for these fields

        Returns:
            (DataQueue): Data queue which contains datapoints from database that satisfy the criteria
        """
        raise NotImplementedError('Method must be implemented by derived class')

class DataQueue:
    """
    Class to define an iterable which returns datapoints in time order.

    All methods must be implemented by derived classes.
    """
    def __init__(
        self,
        data,
        timestamp_field_name
        ):
        """
        Constructor for DataQueue

        Data must be a simple list of dicts containing the datapoints (the
        structure returned by DatabaseConnection.fetch_data()).

        Every datapoint must contain a field with the specified timestamp field name.

        Parameters:
            data (list of dict): Name of the field containing the timestamp for each datapoint
            object_id_field_name (string): Name of the field containing the object ID for each datapoint
            other_field_names (list of string): Names of the remaining fields
        """
        raise NotImplementedError('Method must be implemented by derived class')

    def __iter__(self):
        return self

    def __next__(self):
        """
        Fetch datapoint associated with the next timestamp.

        Data will be fetched in time order.

        Returns:
            (dict): Data associated with the next timestamp
        """
        raise NotImplementedError('Method must be implemented by derived class')

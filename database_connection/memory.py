from . import DatabaseConnection
from . import DataQueue
import dateutil.parser

class DatabaseConnectionMemory(DatabaseConnection):
    """
    Class to define a DatabaseConnection to a database in memory
    """
    def __init__(
        self,
        timestamp_field_name = None,
        object_id_field_name = None
    ):
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

        Parameters:
            timestamp_field_name (string): Name of the field containing the timestamp for each datapoint
            object_id_field_name (string): Name of the field containing the object ID for each datapoint
        """
        self.timestamp_field_name = timestamp_field_name
        self.object_id_field_name = object_id_field_name
        self.data = []

    def write_data(
        self,
        data
    ):
        """
        Write data to the database.

        The data must be in the form of a dictionary with field names as keys
        and data values as values (for a single data point) or a simple list of
        such objects (for multiple datapoints).

        Data values must be serializable/deserializable by the standard JSON
        interface. Any other type/format conversion must be implemented by the
        derived class. Timestamp values (if present) must be given as ISO-format
        strings. Lists are not allowed as data values (to accommodate simple
        implementation as a tabular database).

        Parameters:
            data (dict or list of dict): Data to write to the database
        """
        if not isinstance(data, list):
            data = [data]
        for datum in data:
            if self.timestamp_field_name is not None and self.timestamp_field_name not in datum.keys():
                raise ValueError('Timestamp field \'{}\' not found in datum {}'.format(
                    self.timestamp_field_name,
                    datum
                ))
            if self.object_id_field_name is not None and self.object_id_field_name not in datum.keys():
                raise ValueError('Object ID field \'{}\' not found in datum {}'.format(
                    self.object_id_field_name,
                    datum
                ))
            self.data.append(datum)

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
        if (start_time is not None or end_time is not None) and self.timestamp_field_name is None:
            raise ValueError('Database does not have a designated timestamp field')
        if object_ids is not None and self.object_id_field_name is None:
            raise ValueError('Database does not have a designated object ID field')
        if start_time is not None:
            start_time_datetime = dateutil.parser.parse(start_time)
        if end_time is not None:
            end_time_datetime = dateutil.parser.parse(end_time)
        fetched_data = []
        for datapoint in self.data:
            if start_time is not None and dateutil.parser.parse(datapoint[self.timestamp_field_name]) < start_time_datetime:
                continue
            if end_time is not None and dateutil.parser.parse(datapoint[self.timestamp_field_name]) > end_time_datetime:
                continue
            if object_ids is not None and datapoint[self.object_id_field_name] not in object_ids:
                continue
            fetched_datapoint = {key: value for key, value in datapoint.items() if fields is None or key in fields}
            fetched_data.append(fetched_datapoint)
        return fetched_data

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
        fetched_data = self.fetch_data(
            start_time,
            end_time,
            object_ids,
            fields
        )
        data_queue = DataQueue(
            data = fetched_data,
            timestamp_field_name  = self.timestamp_field_name
        )
        return data_queue

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
            timestamp_field_name (string): Name of the field containing the timestamp for each datapoint
        """
        data.sort(key = lambda datapoint: datapoint[timestamp_field_name])
        self.data = data
        self.timestamp_field_name = timestamp_field_name
        self.num_datapoints = len(data)
        self.next_data_pointer = 0

    def __iter__(self):
        return self

    def __next__(self):
        """
        Fetch datapoint associated with the next timestamp.

        Data will be fetched in time order.

        Returns:
            (dict): Data associated with the next timestamp
        """
        if self.next_data_pointer >= self.num_datapoints:
            raise StopIteration()
        else:
            datapoint = self.data[self.next_data_pointer]
            self.next_data_pointer += 1
            return datapoint

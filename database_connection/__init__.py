import datetime
import dateutil.parser

class DatabaseConnection:
    """
    Class to define a simple, generic database interface that can be adapted to
    different use cases and implementations.

    All methods must be implemented by derived classes.
    """

    def write_data_object_time_series(
        self,
        timestamp,
        object_id,
        data
    ):
        """
        Write data for a given timestamp and object ID.

        Timestamp must either be a native Python datetime or a string which is
        parsable by dateutil.parser.parse(). If timestamp is timezone-naive,
        timezone is assumed to be UTC.

        Data must be serializable by native Python json methods.

        Parameters:
            timestamp (datetime or string): Timestamp associated with data
            object_id (string): Object ID associated with data
            data (dict): Data to be written
        """
        if not self.time_series_database or not self.object_database:
            raise ValueError('Writing data by timestamp and object ID only enabled for object time series databases')
        timestamp = self._python_datetime_utc(timestamp)
        self._write_data_object_time_series(
            timestamp,
            object_id,
            data
        )

    def fetch_data_object_time_series(
        self,
        start_time = None,
        end_time = None,
        object_ids = None
    ):
        """
        Fetch data for a given timespan and set of object IDs.

        If specified, start time and end time must either be native Python
        datetimes or strings which are parsable by dateutil.parser.parse(). If
        they are timezone-naive, they are assumed to be UTC.

        If start time is not specified, all data is returned back to earliest
        data in database. If end time is not specified, all data is returned up
        to most recent data in database. If object IDs are not specified, data
        is returned for all objects.

        Returns a list of dictionaries, one for each datapoint.

        Parameters:
            start_time (datetime or string): Beginning of timespan (default: None)
            end_time (datetime or string): End of timespan (default: None)
            object_ids (list of strings): Object IDs (default: None)

        Returns:
            (list of dict): All data associated with specified time span and object IDs
        """
        if not self.time_series_database or not self.object_database:
            raise ValueError('Fetching data by time interval and/or object ID only enabled for object time series databases')
        if start_time is not None:
            start_time = self._python_datetime_utc(start_time)
        if end_time is not None:
            end_time = self._python_datetime_utc(end_time)
        data = self._fetch_data_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        return data

    def delete_data_object_time_series(
        self,
        start_time,
        end_time,
        object_ids
    ):
        """
        Delete data for a given timespan and set of object IDs.

        Start time, end time, and object IDs must all be specified.

        Start time and end time must either be native Python datetimes or
        strings which are parsable by dateutil.parser.parse(). If they are
        timezone-naive, they are assumed to be UTC.

        Parameters:
            start_time (datetime or string): Beginning of timespan
            end_time (datetime or string): End of timespan
            object_ids (list of strings): Object IDs
        """
        if not self.time_series_database or not self.object_database:
            raise ValueError('Fetching data by time interval and/or object ID only enabled for object time series databases')
        if start_time is None:
            raise ValueError('Start time must be specified for delete data operation')
        if end_time is None:
            raise ValueError('End time must be specified for delete data operation')
        if object_ids is None:
            raise ValueError('Object IDs must be specified for delete data operation')
        start_time = self._python_datetime_utc(start_time)
        end_time = self._python_datetime_utc(end_time)
        self._delete_data_object_time_series(
            start_time,
            end_time,
            object_ids
        )

    def _python_datetime_utc(self, timestamp):
        try:
            if timestamp.tzinfo is None:
                datetime_utc = timestamp.replace(tzinfo = datetime.timezone.utc)
            else:
                datetime_utc = timestamp.astimezone(tz=datetime.timezone.utc)
        except:
            datetime_parsed = dateutil.parser.parse(timestamp)
            if datetime_parsed.tzinfo is None:
                datetime_utc = datetime_parsed.replace(tzinfo = datetime.timezone.utc)
            else:
                datetime_utc = datetime_parsed.astimezone(tz=datetime.timezone.utc)
        return datetime_utc

    def _write_data_object_time_series(
        self,
        timestamp,
        object_id,
        data
    ):
        raise NotImplementedError('Specifics of communication with database must be implemented in child class')

    # Internal method for fetching object time series data (Honeycomb-specific)
    def _fetch_data_object_time_series(
        self,
        start_time,
        end_time,
        object_ids
    ):
        raise NotImplementedError('Specifics of communication with database must be implemented in child class')

    # Internal method for deleting object time series data (Honeycomb-specific)
    def _delete_data_object_time_series(
        self,
        start_time,
        end_time,
        object_ids
    ):
        raise NotImplementedError('Specifics of communication with database must be implemented in child class')

    # def to_data_queue(
    #     self,
    #     start_time = None,
    #     end_time = None,
    #     object_ids = None,
    #     fields = None
    # ):
    #     """
    #     Create an iterable which returns datapoints from the database in time order.
    #
    #     Start time and end time must be ISO-format strings. If start time or end
    #     time is specified and database does not have a designated timestamp
    #     field, an exception will be generated.
    #
    #     If object IDs are specified and database does not have a designated
    #     object ID field, an exception will be generated.
    #
    #     If fields are not specified, all fields are returned.
    #
    #     If database does not have a designated timestamp field, an exception
    #     will be generated.
    #
    #     Parameters:
    #         start_time (string): Return data with timestamps greater than or equal to this value
    #         end_time (string): Return data with timestamps less than or equal to this value
    #         object_ids (list): Return data for these object IDs
    #         fields (list): Return data for these fields
    #
    #     Returns:
    #         (DataQueue): Data queue which contains datapoints from database that satisfy the criteria
    #     """
    #     raise NotImplementedError('Method must be implemented by derived class')

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

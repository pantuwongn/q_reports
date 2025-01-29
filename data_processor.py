from datetime import datetime


def get_data_by_datetime(datetimes=None):
    """
    Fetch data for a given datetime or a list of datetimes.

    :param datetimes: None for current time, or list of datetime strings
    :return: Data corresponding to the given datetime(s)
    """
    if datetimes is None:
        # If no datetime is provided, use the current time
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"Data for current datetime: {dt}"

    # Process a list of datetimes
    results = {}
    for dt in datetimes:
        try:
            # Validate datetime format
            parsed_dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            results[dt] = f"Data for {parsed_dt}"
        except ValueError:
            results[dt] = "Invalid datetime format"

    return results

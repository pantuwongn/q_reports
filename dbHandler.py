import datetime
import logging
import json
import psycopg2
import string
import pytz
import re
from logging.handlers import RotatingFileHandler
from sys import stdout
from uuid import uuid4

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
console_handler = logging.StreamHandler(stdout)
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)

with open('./config.json') as f:
    config = json.load(f)

# dict and list about dataset folder
dataset_id_to_config = config['FOLDER_CONFIG_DICT']
dataset_folder_list = list(dataset_id_to_config.keys())
dataset_to_table_name = {f: f'DATASET_{f}' for f in dataset_folder_list}

# HELPER FUNCTIONS #


def parse_timestamp_from_filename(filename):
    """Parses the timestamp from the filename and converts it to a datetime object."""
    TIMESTAMP_PATTERN = re.compile(r'(\d{8}-\d{6})')
    match = TIMESTAMP_PATTERN.search(filename)
    window_tick_match = re.search(r"_(\d+)\.xml$", filename)

    if match:
        dt = datetime.datetime.strptime(match.group(1), '%Y%m%d-%H%M%S')
        # Define Eastern Time (ET) timezone
        eastern = pytz.timezone('US/Eastern')

        # Localize the datetime object to Eastern Time
        localized_dt = eastern.localize(dt)
        return localized_dt
    elif window_tick_match:
        windows_tick = int(window_tick_match.group(1))
        base_date = datetime.datetime(1, 1, 1)
        dt = base_date + datetime.timedelta(microseconds=windows_tick // 10)
        # Define Eastern Time (ET) timezone
        eastern = pytz.timezone('US/Eastern')

        # Localize the datetime object to Eastern Time
        localized_dt = eastern.localize(dt)
        return localized_dt
    else:
        raise ValueError("Timestamp not found or invalid in filename")


def datetime_to_windows_tick(dt, timezone_name='US/Eastern'):

    # Convert to specified timezone (US/Eastern)
    local_tz = pytz.timezone(timezone_name)
    dt = dt.replace(tzinfo=pytz.utc).astimezone(local_tz)

    # Define the base date (January 1, Year 1)
    base_date = datetime.datetime(1, 1, 1, tzinfo=local_tz)

    # Calculate the difference in microseconds
    delta = dt - base_date
    microseconds = delta.total_seconds() * 1_000_000

    # Convert to Windows tick (100-nanosecond intervals)
    windows_tick = int(microseconds * 10)

    return windows_tick


def strip_whitespace_and_punctuation(text):
    # Define a set of whitespace characters and punctuation
    chars_to_strip = string.whitespace + string.punctuation

    # Strip the characters from the left and right of the string
    return text.strip(chars_to_strip)


def constructParamsString(config, data_dict):
    data_list = []
    for key, data_type in zip(config['keys'], config['data_type']):
        if data_type == 'int':
            data_dict[key](int(data_dict[key]))
        elif data_type == 'string':
            data_list.append(f"'{str(data_dict[key])}'")
        elif data_type == 'float':
            data_list.append(float(data_dict[key]))
        elif data_type == 'bool':
            data_list.append(data_dict[key])
    return ', '.join([str(d) for d in data_list])


def constructDataDict(config, data_dict):
    for key, data_type in zip(config['keys'], config['data_type']):
        if data_dict[key] is None:
            data_dict[key] = data_dict[key]
        elif data_type == 'int':
            data_dict[key] = int(data_dict[key])
        elif data_type == 'string':
            data_dict[key] = strip_whitespace_and_punctuation(
                f"{str(data_dict[key])}")
        elif data_type == 'float':
            data_dict[key] = float(data_dict[key])
        elif data_type == 'bool':
            data_dict[key] = data_dict[key]
        elif data_type == 'timestamp':
            if '.' in data_dict[key]:
                dt = datetime.datetime.strptime(
                    data_dict[key], '%Y-%m-%dT%H:%M:%S.%f')
                data_dict[key] = dt
            else:
                dt = datetime.datetime.strptime(
                    data_dict[key], '%Y-%m-%dT%H:%M:%S')
                data_dict[key] = dt

    return data_dict

######


class DBHandlerPG:
    def __init__(self, url, username, database):
        self.url = url
        self.username = username
        self.database = database

    def run_query(self, sql_query):
        try:
            conn = psycopg2.connect(
                f"dbname='{self.database}' user='{self.username}' host='{self.url}'")
        except:
            logger.error("Cannot connect to the postgresql database")
            raise

        with conn.cursor() as curs:
            try:
                curs.execute(sql_query)
                results = curs.fetchall()
                return results
            # a more robust way of handling errors
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(error)
                raise

    def query_files_with_dynamic_threshold(self, prefix, threshold=None):
        try:
            conn = psycopg2.connect(
                f"dbname='{self.database}' user='{self.username}' host='{self.url}'")
        except:
            logger.error("Cannot connect to the postgresql database")
            raise

        if '11v' in prefix:
            suffix = prefix[3:]
            prefix = '11v'

        # Determine if the threshold is a tick or a timestamp
        is_tick = isinstance(threshold, int)

        if is_tick:
            query = r"""
                    WITH extracted AS (
                        SELECT
                            file_path,
                            file_content,
                            regexp_match(file_path, '^.*[\\/](\w+)_(\d+)\.xml$') AS match_result
                        FROM
                            files
                        WHERE file_path LIKE %s
                    )
                    SELECT
                        file_path,
                        file_content,
                        match_result[1] AS prefix,
                        match_result[2] AS windows_tick
                    FROM
                        extracted
                    WHERE
                        match_result IS NOT NULL AND
                        match_result[1] ~ ('^' || %s || '(_|$)')
                    """
            params = [f"%{prefix}%", prefix]
            if threshold is not None:
                query += " AND CAST(match_result[2] AS BIGINT) < %s"
                params.append(threshold)

            # Add LIMIT clause
            query += " ORDER BY match_result[2] DESC LIMIT 1;"
        else:
            query = r"""
                    WITH extracted AS (
                        SELECT
                            file_path,
                            file_content,
                            regexp_match(file_path, '^.*[\\/](\w+)_.*_(\d{8}-\d{6})\.json$') AS match_result
                        FROM
                            files
                        WHERE file_path LIKE %s
                    )
                    SELECT
                        file_path,
                        file_content,
                        match_result[1] AS prefix,
                        match_result[2] AS timestamp
                    FROM
                        extracted
                    WHERE
                        match_result IS NOT NULL AND
                        match_result[1] ~ ('^' || %s || '(_|$)')
                    """
            params = [f"%{prefix}%", prefix]
            if threshold is not None:
                query += " AND to_timestamp(match_result[2], 'YYYYMMDD-HH24MISS') < %s::timestamp"
                params.append(threshold)

            # Add LIMIT clause
            query += " ORDER BY match_result[2] DESC LIMIT 1;"

        with conn.cursor() as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            if prefix not in ['11v', '11vab']:
                return results
            else:
                filtered_results = []
                for r in results:
                    if prefix == '11v' and 'vab' not in r[0] and f'_{suffix}_' in r[0]:
                        filtered_results.append(r)
                    elif prefix == '11vab' and 'vab' in r[0] and f'_{suffix}_' in r[0]:
                        filtered_results.append(r)
                return filtered_results

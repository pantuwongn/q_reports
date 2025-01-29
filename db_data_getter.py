import datetime
import logging
import json
import numpy as np
import os
import requests
import pandas as pd
import psycopg2
import string
import time
import traceback
import threading
import pytz
import re
import xmltodict
from logging.handlers import RotatingFileHandler
from sys import stdout
from sanity_check import pre_check, post_check, upsert_error
from uuid import uuid4
from questdb.ingress import Sender, IngressError

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create file handler and set level to error
file_handler = RotatingFileHandler(
    'error.log', maxBytes=1024*1024, backupCount=5)
file_handler.setLevel(logging.ERROR)

# Create console handler and set level to debug
console_handler = logging.StreamHandler(stdout)
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
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


def constructParamsString(config, data_dict, collectionTime):
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

    data_list.append(f"'{collectionTime.strftime('%Y-%m-%dT%H:%M:%S')}'")
    return ', '.join([str(d) for d in data_list])


def constructDataDict(config, data_dict, collectionTime):
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
                data_dict[key] = np.datetime64(dt)
            else:
                dt = datetime.datetime.strptime(
                    data_dict[key], '%Y-%m-%dT%H:%M:%S')
                data_dict[key] = np.datetime64(dt)
    # collectionTime.strftime('%Y-%m-%dT%H:%M:%S')

    data_dict['CollectionTime'] = np.datetime64(collectionTime)
    return data_dict

######


class DBHandler:
    def __init__(self, url: str, port: int) -> None:
        self.url = url
        self.port = port

    @ property
    def host(self) -> str:
        return f'{self.url}:{self.port}'

    @ property
    def client_conf(self) -> str:
        h = self.url.split('//')[-1]
        return f'http::addr={h}:{self.port};'

    def import_df(self, df, table_name):
        max_delay = 3600
        delay = 1
        while True:
            try:
                with Sender.from_conf(self.client_conf) as sender:
                    sender.dataframe(df, table_name=table_name,
                                     at='CollectionTime')
                break
            except IngressError as e:
                traceback.print_exception(e)
                print(f' Error during feed df: {e!r} ')
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
            except Exception as e:
                traceback.print_exception(e)
                exit(1)

    def run_query(self, sql_query):
        query_params = {'query': sql_query, 'fmt': 'json'}
        try:
            response = requests.get(self.host + '/exec', params=query_params)
            json_response = json.loads(response.text)
            return json_response
        except requests.exceptions.RequestException as e:
            logging.exception(f'Error during running query: {e}')
            raise


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
                query += " AND CAST(match_result[2] AS BIGINT) > %s"
                params.append(threshold)

            # Add LIMIT clause
            query += " ORDER BY match_result[2] LIMIT 100;"
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
                query += " AND to_timestamp(match_result[2], 'YYYYMMDD-HH24MISS') > %s::timestamp"
                params.append(threshold)

            # Add LIMIT clause
            query += " ORDER BY match_result[2] LIMIT 100;"

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


class DataFeeder(threading.Thread):
    def __init__(self, dataset_id) -> None:
        super().__init__()
        self.dataset_id = dataset_id
        self.stopper = threading.Event()
        self.dbHandler = DBHandler(
            url=config['DB_HOST'], port=config['DB_PORT'])
        self.dbHandlerPG = DBHandlerPG(
            url=config['PG_HOST'], username=config['PG_USER'], database=config['PG_NAME'])

    def stop(self):
        self.stopper.set()

    def init_db(self):
        ''' a method to create table for each dataset in config.json, LastImported, and Error
        '''

        for dataset, d_config in dataset_id_to_config.items():
            for idx, dtype in enumerate(d_config['data_type']):
                if dtype == 'symbol':
                    d_config['data_type'][idx] = 'symbol capacity 256 CACHE'

            sql_statement = f''' 
                CREATE TABLE IF NOT EXISTS '{dataset_to_table_name[dataset]}'
                ({', '.join([f'{d} {t.upper()}' for d, t in zip(d_config['keys'], d_config['data_type'])])} , CollectionTime TIMESTAMP) timestamp (CollectionTime)
                PARTITION BY DAY WAL
                DEDUP UPSERT KEYS({d_config['instrument_key']}, {d_config['timestamp_key']}, CollectionTime);
            '''
            self.dbHandler.run_query(sql_statement)

        # table to keep track last file that fed
        sql_statement = ''' CREATE TABLE IF NOT EXISTS 'LastImported' (dataset_name STRING, last_time_stamp TIMESTAMP)'''
        self.dbHandler.run_query(sql_statement)

    def get_last_feed_timestamp(self, dataset_name):
        sql_statement = f""" SELECT dataset_name, last_time_stamp from LastImported WHERE dataset_name='{
            dataset_name}' """
        last_feed_timestamp = self.dbHandler.run_query(sql_statement)
        if last_feed_timestamp['count'] > 0:
            if dataset_name == 'ind01':
                dt = datetime.datetime.fromisoformat(
                    last_feed_timestamp['dataset'][0][1])
                return datetime_to_windows_tick(dt)
            else:
                return last_feed_timestamp['dataset'][0][1]
        else:
            if dataset_name == 'ind01':
                return 0
            else:
                return None

    def run(self):
        # loop to feed to db every 3 seconds
        while True:
            if self.stopper.isSet():
                return

            dataset_name = self.dataset_id
            last_feed_timestamp = self.get_last_feed_timestamp(dataset_name)

            dataset_table = dataset_name.split('___')[0]

            # query to get list of file contents from postgresql
            # NOTE: to optimize the speed, we limit only 100 records
            try:
                query_response = self.dbHandlerPG.query_files_with_dynamic_threshold(
                    dataset_table, last_feed_timestamp)
            except Exception as e:
                logger.error(f'{e!r}')
                break

            if not query_response:
                # no file content to parse, wait for 10 minutes and try again
                print(f'no query_response for {dataset_name}')
                time.sleep(600)
                continue
            print(last_feed_timestamp)
            print(len(query_response))
            data_dict_list = []
            for record in query_response:
                if dataset_name == 'ind01':
                    collection_time = parse_timestamp_from_filename(
                        os.path.basename(record[0]))
                    try:
                        parsed_dict = xmltodict.parse(record[1])
                        temp_data_list = parsed_dict['DealingInfo']['Currency']
                    except Exception as e:
                        logger.error(f'File content cannot be loaded!')
                        continue

                    if not isinstance(temp_data_list, list):
                        logger.error(
                            f'File content is not in the expected format')
                        continue

                    for temp_data in temp_data_list:
                        data_dict = {}
                        for key, data_type in zip(dataset_id_to_config[dataset_name]['keys'], dataset_id_to_config[dataset_name]['data_type']):
                            if key == 'Symbol':
                                data_dict[key] = temp_data['@Symbol']
                            elif key == 'ValueDate':
                                dt = datetime.datetime.strptime(
                                    temp_data['SSIHistNETME']['ValueDate'], '%Y-%m-%d %H:%M:%S')
                                data_dict[key] = np.datetime64(dt)
                            else:
                                data_dict[key] = float(
                                    temp_data[key]['Value'])
                        data_dict['CollectionTime'] = np.datetime64(
                            collection_time)
                        data_dict_list.append(data_dict)
                else:
                    print(os.path.basename(record[0]))
                    collection_time = parse_timestamp_from_filename(
                        os.path.basename(record[0]))
                    try:
                        temp_data = json.loads(record[1])
                    except Exception as e:
                        logger.error(f'File content cannot be loaded!')
                        continue

                    if not isinstance(temp_data, list):
                        logger.error(
                            f'File content is not in the expected format')
                        continue

                    if '___Server' in dataset_name:
                        id_sp = dataset_name.split('___Server')
                        data = []
                        for td in temp_data:
                            ts = td['time']
                            serverBBI = td['ServerBBI']
                            if serverBBI:
                                for sb in serverBBI:
                                    server_name = sb.pop('Server')
                                    # server_name == f'{id_sp[1]}-Live.mt4tradeserver.com:443' or server_name == f'{id_sp[1]}-live.mt4tradeserver.com:443':
                                    if id_sp[1].lower() in server_name.lower():
                                        data.append({
                                            dataset_id_to_config[self.dataset_id]['timestamp_key']: ts,
                                            **sb
                                        })
                    else:
                        data = temp_data

                    for d in data:
                        if not isinstance(d, dict):
                            continue
                        data_dict = constructDataDict(
                            dataset_id_to_config[self.dataset_id], d, collection_time)
                        data_dict_list.append(data_dict)

            try:
                if data_dict_list:
                    df = pd.DataFrame(data_dict_list)
                    df = df.dropna()

                    self.dbHandler.import_df(df, f'DATASET_{dataset_name}')

                last_feed_timestamp = self.get_last_feed_timestamp(
                    dataset_name)
                if last_feed_timestamp is not None and last_feed_timestamp != 0:
                    sql_statment = f""" UPDATE LastImported SET last_time_stamp='{
                        collection_time}' WHERE dataset_name='{dataset_name}' """
                else:
                    sql_statment = f""" INSERT INTO LastImported (dataset_name, last_time_stamp) VALUES ('{
                        dataset_name}', '{collection_time}') """

                result = self.dbHandler.run_query(sql_statment)
                assert 'error' not in result

                logger.info(f'<=============> Successfully feed {
                            dataset_name} until {collection_time}')
                data_dict_list = []

                # for d in data:
                #     start = time.time()
                #     sql_statment = f""" INSERT INTO FOLDER_{folder_name} ({', '.join(dataset_id_to_config[folder_name]['keys'])}, CollectionTime) VALUES ({constructParamsString(dataset_id_to_config[folder_name], d, last_timestamp_dt)}) """
                #     insert_result = self.dbHandler.run_query( sql_statment )
                #     assert 'error' not in insert_result
                #     print(f'{self.dataset_id}: insert takes {time.time()-start} seconds')

                #     # query for latest 60 records
                #     start = time.time()
                #     sql_statement = f""" SELECT * FROM FOLDER_{folder_name} WHERE {dataset_id_to_config[folder_name]['instrument_key']} = '{d[dataset_id_to_config[folder_name]['instrument_key']]}' ORDER BY CollectionTime DESC LIMIT 60 """
                #     records = self.dbHandler.run_query( sql_statement )['dataset']
                #     try:
                #         post_check( records, folder_name, dataset_id_to_config[folder_name] )
                #     except Exception as e:
                #         logger.error( f'Post-check error!!! {e!r}')
                #     print(f'{self.dataset_id}: post_check takes {time.time()-start} seconds')

                # # update last feed file
                # start = time.time()
                # last_feed_file = self.get_last_feed_file( folder_name )
                # if last_feed_file is not None:
                #     sql_statment = f""" UPDATE LastImported SET last_file_name='{last_file}' WHERE folder_name='{folder_name}' """
                # else:
                #     sql_statment = f""" INSERT INTO LastImported (folder_name, last_file_name) VALUES ('{folder_name}', '{last_file}') """

                # result = self.dbHandler.run_query( sql_statment )
                # assert 'error' not in insert_result
                # print(f'{self.dataset_id}: udate last feed file takes {time.time()-start} seconds')
                # logger.info( f'Successfully feed {file_path}')
            except Exception as e:
                traceback.print_exception(e)
                logger.error(
                    f'Error during parsing file contents : {e!r}')
                continue

            # # POST CEHCK
            # sql_statement = f""" SELECT * FROM FOLDER_{
            #     dataset_name} ORDER BY CollectionTime DESC LIMIT {len(data_dict_list) * 2} """
            # try:
            #     records = self.dbHandler.run_query(sql_statement)
            #     records = records['dataset']
            #     post_check(records, dataset_name,
            #                dataset_id_to_config[dataset_name])
            # except Exception as e:
            #     logger.error(f'Post-check error!!! {e!r}')

            time.sleep(3)

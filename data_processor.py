from datetime import datetime
from dbHandler import DBHandlerPG, datetime_to_windows_tick, constructDataDict
import json
import logging
import xmltodict
from logging.handlers import RotatingFileHandler
from sys import stdout

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

dataset_id_to_config = config['FOLDER_CONFIG_DICT']


def get_data_by_datetime(datetimes=None):
    """
    Fetch data for a given datetime or a list of datetimes.

    :param datetimes: None for current time, or list of datetime strings
    :return: Data corresponding to the given datetime(s)
    """
    dbHandlerPG = DBHandlerPG(
        url=config['PG_HOST'], username=config['PG_USER'], database=config['PG_NAME'])

    if datetimes is None:
        # If no datetime is provided, use the current time
        dt = datetime.now().isoformat()
        datetimes = [dt]
    else:
        for idx in range(len(datetimes)):
            parsed_dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            datetimes[idx] = parsed_dt.isoformat()

    # Process for each dataset for each datetime
    result_dict_list = []
    for dataset_id in dataset_id_to_config:
        for dt in datetimes:
            # if dataset_id is ind01, need to change to windows tick
            isoDt = dt
            if dataset_id == 'ind01':
                dt = datetime_to_windows_tick(dt)

            # query to get a file content of datetime that is previous to dt
            try:
                dataset_table = dataset_id.split('___')[0]
                query_response = dbHandlerPG.query_files_with_dynamic_threshold(
                    dataset_table, dt)
            except Exception as e:
                logger.error(f'{e!r}')
                break

            if not query_response:
                # no file content to parse, wait for 10 minutes and try again
                logger.wanring(f'no record for {
                               dataset_id} for the datetime as {isoDt}')
                continue

            record = query_response[0]

            if dataset_id == 'ind01':
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
                    for key, _ in zip(dataset_id_to_config[dataset_id]['keys'], dataset_id_to_config[dataset_id]['data_type']):
                        if key == 'Symbol':
                            data_dict[key] = temp_data['@Symbol']
                        elif key == 'ValueDate':
                            dt = datetime.datetime.strptime(
                                temp_data['SSIHistNETME']['ValueDate'], '%Y-%m-%d %H:%M:%S')
                            data_dict[key] = dt
                        else:
                            data_dict[key] = float(
                                temp_data[key]['Value'])
                    data_dict['source'] = 'ind01'
                    data_dict['feed'] = ''
                    data_dict_list.append(data_dict)
            else:
                try:
                    temp_data = json.loads(record[1])
                except Exception as e:
                    logger.error(f'File content cannot be loaded!')
                    continue

                if not isinstance(temp_data, list):
                    logger.error(
                        f'File content is not in the expected format')
                    continue

                if '___Server' in dataset_id:
                    id_sp = dataset_id.split('___Server')
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
                                        dataset_id_to_config[dataset_id]['timestamp_key']: ts,
                                        **sb
                                    })
                else:
                    data = temp_data

                for d in data:
                    if not isinstance(d, dict):
                        continue
                    data_dict = constructDataDict(
                        dataset_id_to_config[dataset_id], d)

                    short = data_dict[dataset_id_to_config[dataset_id]['shortKey']]
                    long = data_dict[dataset_id_to_config[dataset_id]['longKey']]
                    bbi = data_dict[dataset_id_to_config[dataset_id]['bbiKey']]
                    if bbi == 'calculate':
                        bbi = max(long, short)/min(long, short)
                        if long < short:
                            bbi = bbi * -1

                    result_dict = {
                        'source': None,
                        'feed': None,
                        'datetime': isoDt,
                        'open': None,
                        'close': None,
                        'high': None,
                        'low': None,
                        'short': short,
                        'long': long,
                        'bbi': bbi
                    }
                    if '___Server' in dataset_id:
                        id_sp = dataset_id.split('___Server')
                        result_dict['source'] = id_sp[0]
                        result_dict['feed'] = id_sp[1]
                    else:
                        result_dict['source'] = dataset_id
                        result_dict['feed'] = ''

                    result_dict_list.append(result_dict)
    return result_dict_list

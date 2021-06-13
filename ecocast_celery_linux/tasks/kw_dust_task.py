import requests
import json
import datetime
from ecocast_conf import logger, psql_connect, psql_select
from ecocast_conf import KWEATHER_HOST, REQUEST_TIME_OUT

""" Kweather Fine Dust Data Task """


def insert_kw_dust():
    """ Receive Kweather's fine dust data and insert it into the database with key
        Args:

        Returns:
            None (None): Success
            True (bool): Retry due to timeout
    """
    conn = psql_connect()
    curs = conn.cursor()
    kw_dust_list = call_kw_dust()
    if kw_dust_list is True:
        return True
    for kw_dust_dict in kw_dust_list:
        values_data = (kw_dust_dict['areaName_wide'], kw_dust_dict['areaName_city'], kw_dust_dict['areaName_dong'],
                       datetime.datetime.strptime(kw_dust_dict['announceTime'], '%Y%m%d%H%M'), kw_dust_dict['Lat'],
                       kw_dust_dict['Lng'], kw_dust_dict['PM10_VALUE'], kw_dust_dict['PM25_VALUE'])
        curs.execute(
            'INSERT INTO kweather_dust(kweather_dust_sd, kweather_dust_sgg, kweather_dust_emd, '
            'kweather_dust_announce_time, kweather_dust_lat, kweather_dust_lon, kweather_dust_pm100, '
            'kweather_dust_pm25) '
            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s)', values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_kw_dust():
    """ Kweather's fine dust key data API call
        Args:

        Returns:
            json_data (list): Kweather's fine dust data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_kw_dust())
            [{'areaName_wide': '서울특별시', 'areaName_city': '종로구', 'areaName_dong': '청운효자동', ... ]
    """
    try:
        response = requests.get(KWEATHER_HOST, timeout=REQUEST_TIME_OUT)
        json_data = response.json()[0]['findust']['station']
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_kw_dust Except : ' + str(e))
        return True
    return json_data


def insert_kw_dust_json():
    """ Receive Kweather's fine dust data and insert it into the database as JSON
        Args:

        Returns:
            None (None): Success
            True (bool): Call null_kw_dust_json due to Request Timeout Except or The database time and API time are the same
    """
    logger.debug('insert_kw_dust_json 실행')
    json_data = call_kw_dust_json()
    if json_data is True:
        return True
    conn = psql_connect()
    select_time_sql = "SELECT kweather_dust_json_data -> 'station' -> 1 ->> 'announceTime' AS announceTime " \
                      "FROM kweather_dust_json ORDER BY kweather_dust_json_date DESC LIMIT 1"
    db_time = psql_select(conn, select_time_sql)
    kw_time = json_data['station'][0]['announceTime']
    if db_time:
        db_time = db_time[0][0]
    else:
        db_time = '2010-01-01 00:00'
    if db_time == kw_time:
        logger.debug('db_time : ' + db_time + ', kw_time : ' + kw_time)
        return True
    values_data = (json.dumps(json_data, ensure_ascii=False),)
    curs = conn.cursor()
    curs.execute('INSERT INTO kweather_dust_json(kweather_dust_json_data) VALUES(%s)', values_data)
    conn.commit()
    curs.close()
    conn.close()


def null_kw_dust_json():
    """ If Kweather's fine dust data is not updated yet
        Args:

        Returns:
            None (None): Success
            True (bool): Retry due to Request Timeout Except or The database time and API time are the same
    """
    logger.debug('null_kw_dust_json 실행')
    json_data = call_kw_dust_json()
    if json_data is True:
        return True
    conn = psql_connect()
    select_time_sql = "SELECT kweather_dust_json_data -> 'station' -> 1 ->> 'announceTime' AS announceTime " \
                      "FROM kweather_dust_json ORDER BY kweather_dust_json_date DESC LIMIT 1"
    db_time = psql_select(conn, select_time_sql)[0][0]
    kw_time = json_data['station'][0]['announceTime']
    if db_time == kw_time:
        logger.debug('db_time : ' + db_time + ', kw_time : ' + kw_time)
        return True
    values_data = (json.dumps(json_data, ensure_ascii=False),)
    curs = conn.cursor()
    curs.execute('INSERT INTO kweather_dust_json(kweather_dust_json_data) VALUES(%s)', values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_kw_dust_json():
    """ Kweather's fine dust JSON data API call
        Args:

        Returns:
            json_data (dict): Kweather's fine dust data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_kw_dust_json())
            {'station': [{'areaName_wide': '서울특별시', 'areaName_city': '종로구', 'areaName_dong': '청운효자동', ... ]}
    """
    try:
        response = requests.get(KWEATHER_HOST, timeout=REQUEST_TIME_OUT)
        json_data = response.json()[0]['findust']
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_kw_dust_json Except : ' + str(e))
        return True
    return json_data

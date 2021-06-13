import requests
from ecocast_conf import logger, psql_connect, psql_dict_select
from ecocast_conf import DATA_WEATHER_HOST, DATA_WEATHER_KEY, REQUEST_TIME_OUT
import datetime

""" Public Data Portal Weather Data Task """


def insert_data_weather_area_list():
    """ List of coordinates x, y to be called to API of the Dataportal's ultra-short-term forecast weather data
        Args:

        Returns:
            area_result (list): X,Y-coordinates in which the column of is_active is true

        Examples:
            >>> print(insert_data_weather_area_list())
            [{'data_weather_area_x': 61.0, 'data_weather_area_y': 126.0}, {'data_weather_area_x': 62.0, ... ]
    """
    logger.debug('insert_data_weather_area_list 실행')
    conn = psql_connect()
    select_area_sql = 'SELECT data_weather_area_x, data_weather_area_y FROM data_weather_area WHERE data_weather_area_is_active = TRUE'
    area_result = psql_dict_select(conn, select_area_sql)
    conn.close()
    return area_result


def insert_data_weather_1_hour(x: int, y: int, now: datetime.datetime):
    """ Receive Dataportal's ultra-short-term forecast weather data and insert it into the database
        Args:
            x (int): X-coordinate of area to be called
            y (int): Y-coordinate of area to be called
            now (datetime.datetime): Current Time

        Returns:
            None (None): Success
            True (bool): Retry due to Request Timeout Except
    """
    logger.debug('insert_data_weather_1_hour(' + str(x) + ',' + str(y) + ') 실행')
    base_date = now.strftime('%Y%m%d')
    base_time = str(int(now.strftime('%H') + '30'))
    if len(base_time) < 4:
        base_time = '0' + base_time
    weather_list = call_data_weather_1_hour(x, y, base_date, base_time)
    if weather_list is True:
        return True
    weather_data_list = data_weather_conversion(x, y, base_date, base_time, weather_list)
    conn = psql_connect()
    curs = conn.cursor()
    for weather_data_dict in weather_data_list:
        values_data = (
            datetime.datetime.strptime(weather_data_dict['baseDate'] + weather_data_dict['baseTime'], '%Y%m%d%H%M'),
            weather_data_dict['LGT'], weather_data_dict['PTY'], weather_data_dict['RN1'],
            weather_data_dict['SKY'], weather_data_dict['T1H'], weather_data_dict['REH'],
            weather_data_dict['UUU'], weather_data_dict['VVV'], weather_data_dict['VEC'],
            weather_data_dict['WSD'],
            datetime.datetime.strptime(weather_data_dict['fcstDate'] + weather_data_dict['fcstTime'], '%Y%m%d%H%M'),
            weather_data_dict['nx'], weather_data_dict['ny'])
        curs.execute(
            'INSERT INTO data_weather_1_hour(data_weather_1_hour_date, data_weather_1_hour_lgt, '
            'data_weather_1_hour_pty, data_weather_1_hour_rn1, data_weather_1_hour_sky, '
            'data_weather_1_hour_t1h, data_weather_1_hour_reh, data_weather_1_hour_uuu, '
            'data_weather_1_hour_vvv, data_weather_1_hour_vec, data_weather_1_hour_wsd, '
            'data_weather_1_hour_forecast_date, data_weather_area_x, data_weather_area_y) '
            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_data_weather_1_hour(x: int, y: int, base_date: str, base_time: str):
    """ Dataportal's ultra-short-term forecast weather data API call
        Args:
            x (int): X-coordinate of area to be called
            y (int): Y-coordinate of area to be called
            base_date (str): Current date
            base_time (str): Current time

        Returns:
            json_data['response']['body']['items']['item'] (list): Dataportal's ultra-short-term forecast weather data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_data_weather_1_hour(60, 127, '20201026', '2030'))
            [{'baseDate': '20201026', 'baseTime': '2030', 'category': 'LGT', 'fcstDate': '20201026', 'fcstTime': '2100', ... ]
    """
    logger.debug('call_data_weather_1_hour(' + str(x) + ', ' + str(y) + ') 실행')
    url = DATA_WEATHER_HOST + 'getUltraSrtFcst'
    param = {'serviceKey': DATA_WEATHER_KEY, 'numOfRows': 100, 'pageNo': 1, 'dataType': 'JSON',
             'base_date': base_date, 'base_time': base_time, 'nx': x, 'ny': y}
    try:
        response = requests.get(url, param, timeout=REQUEST_TIME_OUT)
        json_data = response.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_data_weather_1_hour(' + str(x) + ', ' + str(y) + ') Except : ' + str(e))
        return True
    return json_data['response']['body']['items']['item']


def insert_data_weather_3_hour(x: int, y: int, now: datetime.datetime):
    """ Receive Dataportal's neighborhood forecast weather data and insert it into the database
        Args:
            x (int): X-coordinate of area to be called
            y (int): Y-coordinate of area to be called
            now (datetime.datetime): Current Time

        Returns:
            None (None): Success
            True (bool): Retry due to Request Timeout Except
    """
    logger.debug('insert_data_weather_3_hour(' + str(x) + ',' + str(y) + ') 실행')
    base_date = now.strftime('%Y%m%d')
    base_time = str(int(now.strftime('%H') + '00'))
    if len(base_time) < 4:
        base_time = '0' + base_time
    weather_list = call_data_weather_3_hour(x, y, base_date, base_time)
    if weather_list is True:
        return True
    weather_data_list = data_weather_conversion(x, y, base_date, base_time, weather_list)
    conn = psql_connect()
    curs = conn.cursor()
    for weather_data_dict in weather_data_list:
        values_data = (
            datetime.datetime.strptime(weather_data_dict['baseDate'] + weather_data_dict['baseTime'], '%Y%m%d%H%M'),
            weather_data_dict['POP'], weather_data_dict['PTY'], weather_data_dict['REH'],
            weather_data_dict['SKY'], weather_data_dict['T3H'], weather_data_dict['UUU'],
            weather_data_dict['VEC'], weather_data_dict['VVV'], weather_data_dict['WSD'],
            datetime.datetime.strptime(weather_data_dict['fcstDate'] + weather_data_dict['fcstTime'], '%Y%m%d%H%M'),
            weather_data_dict['nx'], weather_data_dict['ny'])
        curs.execute(
            'INSERT INTO data_weather_3_hour(data_weather_3_hour_date, data_weather_3_hour_pop, '
            'data_weather_3_hour_pty, data_weather_3_hour_reh, data_weather_3_hour_sky, '
            'data_weather_3_hour_t3h, data_weather_3_hour_uuu, data_weather_3_hour_vec, '
            'data_weather_3_hour_vvv, data_weather_3_hour_wsd, data_weather_3_hour_forecast_date, '
            'data_weather_area_x, data_weather_area_y) '
            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_data_weather_3_hour(x: int, y: int, base_date: str, base_time: str):
    """ Dataportal's neighborhood forecast weather data API call
        Args:
            x (int): X-coordinate of area to be called
            y (int): Y-coordinate of area to be called
            base_date (str): Current date
            base_time (str): Current time

        Returns:
            json_data['response']['body']['items']['item'] (list): Dataportal's neighborhood forecast weather data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_data_weather_3_hour(60, 127, '20201027', '1400'))
            [{'baseDate': '20201027', 'baseTime': '1400', 'category': 'POP', 'fcstDate': '20201027', ... ]
    """
    logger.debug('call_data_weather_3_hour(' + str(x) + ', ' + str(y) + ') 실행')
    url = DATA_WEATHER_HOST + 'getVilageFcst'
    param = {'serviceKey': DATA_WEATHER_KEY, 'numOfRows': 250, 'pageNo': 1, 'dataType': 'JSON',
             'base_date': base_date, 'base_time': base_time, 'nx': x, 'ny': y}
    try:
        response = requests.get(url, param, timeout=REQUEST_TIME_OUT)
        json_data = response.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_data_weather_3_hour(' + str(x) + ', ' + str(y) + ') Except : ' + str(e))
        return True
    return json_data['response']['body']['items']['item']


def data_weather_conversion(x: int, y: int, base_date: str, base_time: str, data_dict: dict):
    """ Dataportal's weather data Dict conversion to List
        Args:
            x (int): X-coordinate of area to be called
            y (int): Y-coordinate of area to be called
            base_date (str): Current date
            base_time (str): Current time
            data_dict: Recalled Weather data from the Dataportal

        Returns:
            weather_list (list): Clean up data by date and time and convert from dict data type to list data type

        Examples:
            >>> x_ = 60
            >>> y_ = 127
            >>> now_ = datetime.datetime.now()
            >>> base_date = now.strftime('%Y%m%d')
            >>> base_time = str(int(now.strftime('%H') + '00'))
            >>> if len(base_time) < 4:
            >>>     base_time = '0' + base_time
            >>> area_ = call_data_weather_1_hour(x_, y_, base_date, base_time)
            >>> print(area_)
            [{'baseDate': '20201027', 'baseTime': '1130', ... }, {'baseDate': '20201027', 'baseTime': '1130', ...]
            >>> print(data_weather_conversion(x, y, base_date, base_time, area_)
            [{'baseDate': '20201027', 'baseTime': '1130', 'fcstDate': '20201027', 'fcstTime': '1200', 'nx': 60, 'ny': 127, 'LGT': '0', 'PTY': '0', ... ]
    """
    forecast_time_dict = {}
    for forecast_time in data_dict:
        key_name = str(forecast_time['fcstDate']) + str(forecast_time['fcstTime'])
        forecast_time_dict[key_name] = (forecast_time['fcstDate'], forecast_time['fcstTime'])

    hourly_data_dict = {}
    for key, val in forecast_time_dict.items():
        hourly_data_dict[key] = {'baseDate': base_date, 'baseTime': base_time, 'fcstDate': val[0],
                                 'fcstTime': val[1], 'nx': x, 'ny': y}

    for key in hourly_data_dict:
        for weather in data_dict:
            if (str(weather['fcstDate']) + str(weather['fcstTime'])) == key:
                hourly_data_dict[key].update({weather['category']: weather['fcstValue']})

    weather_list = list(hourly_data_dict.values())

    return weather_list

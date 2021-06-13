import requests
import json
from ecocast_conf import logger, psql_connect
from ecocast_conf import OPENWEATHER_HOST, OPENWEATHER_KEY, REQUEST_TIME_OUT

""" Openweather Weather Data Task """

"""
서울 경기권 지역은 약 230개 데이터 정도면 충분할듯(공공데이터포털의 동네예보 지역 참고)
오픈웨더 한달에 3만건가능
24 * 30 = 720 : 1지역당 한달에 720번 호출 -> 약 41.6개지역 1시간마다 24번
8 * 30 = 240 : 1지역당 한달에 240번 호출 -> 약 125개 지역 00, 03, 06, 09, 12, 15, 18, 21
6 * 30 = 180 : 1지역당 한달에 180번 호출 -> 약 166.6개 지역 00, 04, 08, 12, 16, 20
4 * 30 = 120 : 1지역당 한달에 120번 호출 -> 약 250개 지역 00, 06, 12, 18 4번
3 * 30 = 90 -> 약 333.3개 00, 08, 16 3번 -> 300개의 데이터 저장시 1.2만번 더 호출가능
2 * 30 = 60 -> 약 500개 00, 12 2번 -> 300개의 데이터 저장시 1.2만번 더 호출가능
"""


def insert_ow_weather_json(lat: float, lon: float):
    """ Receive Openweather's weather data and insert it into the database as JSON
        Args:
            lat (float): Latitude of the area to be called
            lon (float): Longitude of the area to be called

        Returns:
            None (None): Success
            True (bool): Retry due to Request Timeout Except
    """
    logger.debug('insert_ow_weather_json 실행')
    json_data = call_ow_weather_json(lat, lon)
    if json_data is True:
        return True
    conn = psql_connect()
    values_data = (json.dumps(json_data, ensure_ascii=False),)
    curs = conn.cursor()
    curs.execute('INSERT INTO openweather_weather_json(openweather_weather_json_data) VALUES(%s)', values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_ow_weather_json(lat: float, lon: float):
    """ Openweather's fine dust key data API call
        Args:
            lat (float): Latitude of the area to be called
            lon (float): Longitude of the area to be called

        Returns:
            json_data (dict): Openweather's weather data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_ow_weather_json(37.496550, 127.024774))
            {'lat': 37.5, 'lon': 127.02, 'timezone': 'Asia/Seoul', 'timezone_offset': 32400, 'current': {'dt': 1603709877, ... }
    """
    logger.debug('call_ow_weather_json 실행')
    params_ = {'lat': lat, 'lon': lon, 'exclude': '', 'appid': OPENWEATHER_KEY, 'units': 'metric', 'lang': 'kr'}
    try:
        response = requests.get(OPENWEATHER_HOST, params_, timeout=REQUEST_TIME_OUT)
        json_data = response.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_ow_weather_json Except : ' + str(e))
        return True
    return json_data

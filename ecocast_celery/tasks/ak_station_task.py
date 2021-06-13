import requests
from ..ecocast_conf import logger, psql_connect
from ..ecocast_conf import AIRKOREA_HOST, AIRKOREA_KEY, REQUEST_TIME_OUT

""" Airkorea Station Data Task """


def insert_ak_station():
    """ Receive AirKorea's station data and insert it into the database
        Args:

        Returns:
            True (bool): Retry due to null data
            None (None): Success
    """
    logger.debug('insert_ak_station 실행')
    conn = psql_connect()
    curs = conn.cursor()
    station_list = call_ak_station()
    if station_list is True:
        return True
    for ak_station_dict in station_list:
        if ak_station_dict['dmX'] == '':
            ak_station_dict['dmX'] = -1
        if ak_station_dict['dmY'] == '':
            ak_station_dict['dmY'] = -1
        if ak_station_dict['year'] == '':
            ak_station_dict['year'] = 0
        values_data = (
            ak_station_dict['addr'], ak_station_dict['dmX'], ak_station_dict['dmY'],
            ak_station_dict['item'], ak_station_dict['mangName'],
            ak_station_dict['stationName'], ak_station_dict['year'])
        curs.execute(
            'INSERT INTO airkorea_station(airkorea_station_address, airkorea_station_lat, airkorea_station_lon, '
            'airkorea_station_item, airkorea_station_mang, airkorea_station_name, airkorea_station_year) '
            'VALUES(%s, %s, %s, %s, %s, %s, %s)', values_data)
    conn.commit()
    curs.close()
    conn.close()  # DB_POOL.putconn(conn)


def call_ak_station():
    """ Airkorea's station data API call
        Args:

        Returns:
            json_data['list'] (list): Airkorea's station data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_ak_station())
            [{'_returnType': 'json', 'addr': '경남 창원시 의창구 원이대로 450(시설관리공단 실내수영장 앞)', 'districtNum': '', ... ]
    """
    logger.debug('call_ak_station 실행')
    url = AIRKOREA_HOST + 'MsrstnInfoInqireSvc/getMsrstnList'
    params_ = {'serviceKey': AIRKOREA_KEY, 'numOfRows': 700, 'pageNo': 1, 'returnType': 'json'}
    try:
        response = requests.get(url, params_, timeout=REQUEST_TIME_OUT)
        json_data = response.json()  # json_data = json.loads(response.text)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_ak_station Except : ' + str(e))
        return True
    return json_data['response']['body']['items']

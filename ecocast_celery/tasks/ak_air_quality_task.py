import requests
from ..ecocast_conf import logger, psql_connect, psql_select
from ..ecocast_conf import AIRKOREA_HOST, AIRKOREA_KEY, REQUEST_TIME_OUT

""" Airkorea Air Quality Data Task """


def insert_ak_air_quality_list():
    """ List of station names to be called to API of the Airkorea's air quality data
        Args:

        Returns:
            station_result (list): Station name in which the column of is_active is true
            True (bool): Request Timeout Except or The database time and API time are the same

        Examples:
            >>> print(insert_ak_air_quality_list)
            [('고읍',), ('중구',), ('한강대로',), ('종로구',), ... ]
    """
    logger.debug('insert_ak_air_quality_list 실행')
    conn = psql_connect()
    select_time_sql = 'SELECT airkorea_air_quality_time FROM airkorea_air_quality ' \
                      'ORDER BY airkorea_air_quality_id DESC LIMIT 1'
    select_station_sql = 'SELECT airkorea_station_name FROM airkorea_station WHERE airkorea_station_is_active = TRUE'
    time_result = psql_select(conn, select_time_sql)
    station_result = psql_select(conn, select_station_sql)
    conn.close()
    if time_result:
        db_time = time_result[0][0].strftime('%Y-%m-%d %H:%M')
    else:
        db_time = '2010-01-01 00:00'
    try:
        ak_time = call_ak_air_quality(station_result[0][0])[0]['dataTime']
    except TypeError:
        return True
    if db_time == ak_time:
        logger.debug('db_time : ' + db_time + ', ak_time : ' + ak_time)
        return True
    return station_result


def insert_ak_air_quality(station_name: str):
    """ Receive AirKorea's air quality data and insert it into the database
        Args:
            station_name (str): Station name of Station data provided by Airkorea

        Returns:
            None (None): Success or no station data
            Retry (str): Retry due to Request Timeout Except
            True (bool): Call null_ak_air_quality due to null data
    """
    logger.debug('insert_ak_air_quality(' + station_name + ') 실행')
    try:
        air_quality_data = call_ak_air_quality(station_name)[0]
    except IndexError as ie:
        logger.debug('insert_ak_air_quality(' + station_name + ') Except : ' + str(ie))
        return None
    except TypeError:
        return 'Retry'
    values_data = (
        air_quality_data['dataTime'], air_quality_data['mangName'], air_quality_data['so2Value'],
        air_quality_data['so2Grade'],
        air_quality_data['coValue'], air_quality_data['coGrade'], air_quality_data['o3Value'],
        air_quality_data['o3Grade'],
        air_quality_data['no2Value'], air_quality_data['no2Grade'], air_quality_data['pm10Value'],
        air_quality_data['pm10Grade'],
        air_quality_data['pm10Value24'], air_quality_data['pm25Value'], air_quality_data['pm25Grade'],
        air_quality_data['pm25Value24'],
        air_quality_data['khaiValue'], air_quality_data['khaiGrade'], station_name)
    conn = psql_connect()
    curs = conn.cursor()
    curs.execute(
        'INSERT INTO airkorea_air_quality(airkorea_air_quality_time, airkorea_air_quality_mang, '
        'airkorea_air_quality_so2, airkorea_air_quality_so2_grade, airkorea_air_quality_co, '
        'airkorea_air_quality_co_grade, airkorea_air_quality_o3, airkorea_air_quality_o3_grade, '
        'airkorea_air_quality_no2, airkorea_air_quality_no2_grade, '
        'airkorea_air_quality_pm10, airkorea_air_quality_pm10_grade, airkorea_air_quality_pm10_forecast, '
        'airkorea_air_quality_pm25, airkorea_air_quality_pm25_grade, airkorea_air_quality_pm25_forecast, '
        'airkorea_air_quality_khai, airkorea_air_quality_khai_grade, airkorea_station_name) '
        'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values_data)
    conn.commit()
    curs.close()
    conn.close()
    for value_data in values_data:
        if value_data == '' or value_data == '-':
            return True


def null_ak_air_quality(station_name: str):
    """ If receive AirKorea's air quality data in null value

        Args:
            station_name (str): Station name of station data provided by Airkorea

        Returns:
            None (None): Success or no station data
            True (bool): Retry due to null data
    """
    logger.debug('null_ak_air_quality(' + station_name + ') 실행')
    try:
        air_quality_data = call_ak_air_quality(station_name)[0]
    except IndexError as e:
        logger.debug('null_ak_air_quality(' + station_name + ') Except : ' + str(e))
        return None
    except TypeError:
        return True
    values_data = (
        air_quality_data['so2Value'], air_quality_data['so2Grade'],
        air_quality_data['coValue'], air_quality_data['coGrade'], air_quality_data['o3Value'],
        air_quality_data['o3Grade'],
        air_quality_data['no2Value'], air_quality_data['no2Grade'], air_quality_data['pm10Value'],
        air_quality_data['pm10Grade'],
        air_quality_data['pm10Value24'], air_quality_data['pm25Value'], air_quality_data['pm25Grade'],
        air_quality_data['pm25Value24'],
        air_quality_data['khaiValue'], air_quality_data['khaiGrade'], air_quality_data['dataTime'], station_name)
    for value_ in values_data:
        if value_ == '' or value_ == '-':
            return True
    conn = psql_connect()
    curs = conn.cursor()
    curs.execute(
        'UPDATE airkorea_air_quality SET airkorea_air_quality_so2 = %s, airkorea_air_quality_so2_grade = %s, '
        'airkorea_air_quality_co = %s, airkorea_air_quality_co_grade = %s, airkorea_air_quality_o3 = %s, '
        'airkorea_air_quality_o3_grade = %s, airkorea_air_quality_no2 = %s, airkorea_air_quality_no2_grade = %s, '
        'airkorea_air_quality_pm10 = %s, airkorea_air_quality_pm10_grade = %s, airkorea_air_quality_pm10_forecast = %s, '
        'airkorea_air_quality_pm25 = %s, airkorea_air_quality_pm25_grade = %s, '
        'airkorea_air_quality_pm25_forecast = %s, airkorea_air_quality_khai = %s, airkorea_air_quality_khai_grade = %s '
        'WHERE airkorea_air_quality_time = %s AND airkorea_station_name = %s', values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_ak_air_quality(station_name: str):
    """ Airkorea's air quality data API call
        Args:
            station_name (str): Station name of station data provided by Airkorea

        Returns:
            json_data['list'] (list): Airkorea's air quality data
            True (bool): Request Timeout Except

        Examples:
            >>> print(call_ak_air_quality('고읍'))
            [{'_returnType': 'json', 'coGrade': '1', 'coValue': '0.6', 'dataTerm': '', ... ]
    """
    logger.debug('call_ak_air_quality(' + station_name + ') 실행')
    url = AIRKOREA_HOST + 'ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
    params_ = {'serviceKey': AIRKOREA_KEY, 'numOfRows': 1, 'pageNo': 1, 'stationName': station_name,
               'dataTerm': 'DAILY',
               'ver': 1.3, 'returnType': 'json'}
    try:
        response = requests.get(url, params_, timeout=REQUEST_TIME_OUT)
        json_data = response.json()
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        logger.debug('call_ak_air_quality(' + station_name + ') Except : ' + str(e))
        return True
    return json_data['response']['body']['items']

import requests
from psycopg2.pool import ThreadedConnectionPool
import psycopg2
import json
from celery import Celery
import datetime
# from datetime import timedelta
from celery.schedules import crontab
import logging
# import os
from time import sleep
import random

""" Celery Main File
    Version: 1.0
    Author: Ecoplay 김유빈
    Date: 2020.10.16
    Comment:
        1. Airkorea Data: 1시간 주기, Kweather Data: 5분 주기
        2. 공공데이터포털에서 제공하는 인증키(AIRKOREA_KEY)는 URL인코딩 상태의 키를 제공, 
        URL인코딩된 키를 다시 인코딩 하기 때문에 디코딩된 데이터로 바꿔서 넣어야됩니다.

    ToDo:
        1. 날씨정보 api 추가
"""

# os.system('celery beat -A api_data -l DEBUG')

# if not (os.path.isdir('log')):
#     os.mkdir(os.path.join('log'))

logger = logging.getLogger(__name__)
log_format = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
file_max_bytes = 10 * 1024 * 1024
log_file_handler = logging.handlers.RotatingFileHandler('./log/api_data_celery_log.log', maxBytes=file_max_bytes,
                                                        backupCount=20)
log_file_handler.setFormatter(log_format)
logger.addHandler(log_file_handler)
logger.setLevel(level=logging.DEBUG)

AIRKOREA_HOST = 'http://openapi.airkorea.or.kr/openapi/services/rest/'
AIRKOREA_KEY = 'AIRKOREA_KEY'
KWEATHER_HOST = 'http://hosting.kweather.co.kr/bigdata/digital_air/digital_air_json.php?' \
                'api_key=KWEATHER_KEY'
DB_HOST = 'DB_HOST'
DB_NAME = 'DB_NAME'
DB_USER = 'DB_USER'
DB_PW = 'DB_PW'
DB_PORT = 'DB_PORT'
DB_POOL = ThreadedConnectionPool(1, 30, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)


def psql_connect():
    """ PostgreSQL Connection Pool
        Args:

        Returns:
            conn (object): connection Object
    """
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)
    if DB_POOL:
        return DB_POOL.getconn()
    else:
        return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)


def psql_select(conn, sql):
    """ PostgreSQL Connection Pool
        Args:
            conn (object): connection Object
            sql (str): SQL Query
        Returns:
            curs.fetchall() (object): Result Data due to SELECT
    """
    curs = conn.cursor()
    curs.execute(sql)
    result = curs.fetchall()
    curs.close()
    return result


app = Celery('api_data', backend='rpc://', broker='redis://')

""" Celery Beat로 일정주기, 시간마다 특정 작업 실행 
    '<ScheduleName>': {
            'task': '<ModuleName>.<FunctionName>',
            'schedule': <ScheduleTime(timedelta or crontab>,
            'args': ()
        },
    timedelta는 일정시간마다 반복이고, crontab은 일정시간에 실행합니다.
"""
app.conf.update(
    CELERY_TIMEZONE='Asia/Seoul',
    CELERY_ENABLE_UTC=False,
    CELERYBEAT_SCHEDULE={
        'insert_ak_air_quality_list-1-hour': {
            'task': 'api_data.insert_ak_air_quality_list',
            'schedule': crontab(minute='1', hour='*'),  # timedelta(hours=1)
            'args': ()
        },
        'insert_kw_dust_json-5-minute': {
            'task': 'api_data.insert_kw_dust_json',
            'schedule': crontab(minute='1,6,11,16,21,26,31,36,41,46,51,56'),  # timedelta(minutes=5)
            'args': ()
        },
    }
)


def call_ak_station():
    """ Airkorea의 측정소 정보 데이터 API 호출
        Args:

        Returns:
            json_data['list'] (list): Airkorea의 측정소 정보 데이터
    """
    url = AIRKOREA_HOST + 'MsrstnInfoInqireSvc/getMsrstnList'
    param = {'serviceKey': AIRKOREA_KEY, 'numOfRows': 600, 'pageNo': 1, '_returnType': 'json'}
    response = requests.get(url, param)
    json_data = response.json()  # json_data = json.loads(response.text)
    return json_data['list']


@app.task(bind=True, queue='main', expires=60)
def insert_ak_station(self):
    """ Airkorea의 측정소 정보 데이터를 받아와 Database에 Insert
        Args:

        Returns:
    """
    logger.debug('insert_ak_station 실행')
    conn = psql_connect()
    curs = conn.cursor()
    for station_list in call_ak_station():
        if station_list['dmX'] == "":
            station_list['dmX'] = -1
        if station_list['dmY'] == "":
            station_list['dmY'] = -1
        if station_list['year'] == "":
            station_list['year'] = 0
        values_data = (
            station_list['addr'], station_list['dmX'], station_list['dmY'], station_list['item'],
            station_list['mangName'],
            station_list['map'], station_list['oper'],
            station_list['photo'],
            station_list['stationName'], station_list['year'])
        curs.execute(
            'INSERT INTO airkorea_station(airkorea_station_address, airkorea_station_lat, airkorea_station_lon, '
            'airkorea_station_item, airkorea_station_mang, airkorea_station_map, airkorea_station_oper, '
            'airkorea_station_photo, airkorea_station_name, airkorea_station_year) '
            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values_data)
    conn.commit()
    curs.close()
    conn.close()  # DB_POOL.putconn(conn)


@app.task(bind=True, queue='main', expires=1200)
def insert_ak_air_quality_list(self):
    """ Airkorea의 측정소 정보 데이터 중 API로 호출될 측정소명 SELECT
        Args:

        Returns:
    """
    logger.debug('insert_ak_air_quality_list 실행')
    conn = psql_connect()
    select_time_sql = 'SELECT airkorea_air_quality_time FROM airkorea_air_quality ORDER BY airkorea_air_quality_id DESC LIMIT 1'
    select_station_sql = 'SELECT airkorea_station_name FROM airkorea_station WHERE airkorea_station_is_active = TRUE'
    time_result = psql_select(conn, select_time_sql)
    station_result = psql_select(conn, select_station_sql)
    conn.close()
    if time_result:
        db_time = time_result[0][0].strftime('%Y-%m-%d %H:%M')
    else:
        db_time = '2010-01-01 00:00'
    ak_time = call_ak_air_quality(station_result[0][0])[0]['dataTime']
    if db_time == ak_time:
        print("db_time : " + db_time + ", ak_time : " + ak_time)
        self.retry(countdown=(3 * 60), max_retries=15, queue='main', time_limit=1400)
        return
    for station in station_result:
        insert_ak_air_quality.apply_async(args=[station[0]], queue='main', expires=60)
        sleep(random.randint(2, 5))


def call_ak_air_quality(station_name):
    """ Airkorea의 공기질 정보 데이터 API 호출
        Args:
            station_name (str): Airkorea에서 제공하는 측정소 정보 데이터의 측정소명

        Returns:
            json_data['list'] (list): Airkorea의 공기질 정보 데이터
    """
    logger.debug('call_ak_air_quality(' + station_name + ') 실행')
    url = AIRKOREA_HOST + 'ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
    param = {'serviceKey': AIRKOREA_KEY, 'numOfRows': 1, 'pageNo': 1, 'stationName': station_name, 'dataTerm': 'DAILY',
             'ver': 1.3, '_returnType': 'json'}
    # param['numOfRows'] = 30
    response = requests.get(url, param)
    json_data = response.json()
    return json_data['list']


@app.task(bind=True, queue='main', expires=70)
def insert_ak_air_quality(self, station_name):
    """ Airkorea의 공기질 정보 데이터를 받아와 Database에 Insert
        Args:
            station_name (str): Airkorea에서 제공하는 측정소 정보 데이터의 측정소명

        Returns:
    """
    logger.debug('insert_ak_air_quality(' + station_name + ') 실행')
    air_quality_data = call_ak_air_quality(station_name)[0]
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
        "INSERT INTO airkorea_air_quality(airkorea_air_quality_time, airkorea_air_quality_mang, "
        "airkorea_air_quality_so2, airkorea_air_quality_so2_grade, airkorea_air_quality_co, "
        "airkorea_air_quality_co_grade, airkorea_air_quality_o3, airkorea_air_quality_o3_grade, "
        "airkorea_air_quality_no2, airkorea_air_quality_no2_grade, "
        "airkorea_air_quality_pm10, airkorea_air_quality_pm10_grade, airkorea_air_quality_pm10_forecast, "
        "airkorea_air_quality_pm25, airkorea_air_quality_pm25_grade, airkorea_air_quality_pm25_forecast, "
        "airkorea_air_quality_khai, airkorea_air_quality_khai_grade, airkorea_station_name) "
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values_data)
    conn.commit()
    curs.close()
    conn.close()
    for value_data in values_data:
        if value_data == '' or value_data == '-':
            null_ak_air_quality.apply_async(countdown=(3 * 60), args=[station_name], queue='sub', expires=60)
            break


@app.task(bind=True, queue='sub', max_retries=15, time_limit=60)
def null_ak_air_quality(self, station_name):
    """ Airkorea에서 받아온 공기질 정보 데이터가 null 값이 들어가 있을때
        Args:
            station_name (str): Airkorea에서 제공하는 측정소 정보 데이터의 측정소명

        Returns:
    """
    logger.debug('null_ak_air_quality(' + station_name + ') 실행')
    air_quality_data = call_ak_air_quality(station_name)[0]
    values_data = (
        air_quality_data['so2Value'], air_quality_data['so2Grade'],
        air_quality_data['coValue'], air_quality_data['coGrade'], air_quality_data['o3Value'],
        air_quality_data['o3Grade'],
        air_quality_data['no2Value'], air_quality_data['no2Grade'], air_quality_data['pm10Value'],
        air_quality_data['pm10Grade'],
        air_quality_data['pm10Value24'], air_quality_data['pm25Value'], air_quality_data['pm25Grade'],
        air_quality_data['pm25Value24'],
        air_quality_data['khaiValue'], air_quality_data['khaiGrade'], air_quality_data['dataTime'], station_name)
    for value_data in values_data:
        if value_data == '' or value_data == '-':
            self.retry(countdown=(3 * 60), max_retries=14, queue='sub', time_limit=60)
            return
    conn = psql_connect()
    curs = conn.cursor()
    curs.execute(
        "UPDATE airkorea_air_quality SET airkorea_air_quality_so2 = %s, airkorea_air_quality_so2_grade = %s, "
        "airkorea_air_quality_co = %s, airkorea_air_quality_co_grade = %s, airkorea_air_quality_o3 = %s, "
        "airkorea_air_quality_o3_grade = %s, airkorea_air_quality_no2 = %s, airkorea_air_quality_no2_grade = %s, "
        "airkorea_air_quality_pm10 = %s, airkorea_air_quality_pm10_grade = %s, airkorea_air_quality_pm10_forecast = %s, "
        "airkorea_air_quality_pm25 = %s, airkorea_air_quality_pm25_grade = %s, "
        "airkorea_air_quality_pm25_forecast = %s, airkorea_air_quality_khai = %s, airkorea_air_quality_khai_grade = %s "
        "WHERE airkorea_air_quality_time = %s AND airkorea_station_name = %s", values_data)
    conn.commit()
    curs.close()
    conn.close()


@app.task(bind=True, queue='main', expires=60)
def insert_ak_air_quality_all(self, station_name):
    conn = psql_connect()
    curs = conn.cursor()
    for station_list in call_ak_air_quality(station_name):
        values_data = (
            station_list['dataTime'], station_list['mangName'], station_list['so2Value'], station_list['so2Grade'],
            station_list['coValue'], station_list['coGrade'],
            station_list['o3Value'], station_list['o3Grade'],
            station_list['no2Value'], station_list['no2Grade'],
            station_list['pm10Value'], station_list['pm10Grade'], station_list['pm10Value24'],
            station_list['pm25Value'], station_list['pm25Grade'], station_list['pm25Value24'],
            station_list['khaiValue'], station_list['khaiGrade'], station_name)
        curs.execute(
            "INSERT INTO airkorea_air_quality(airkorea_air_quality_time, airkorea_air_quality_mang, "
            "airkorea_air_quality_so2, airkorea_air_quality_so2_grade, "
            "airkorea_air_quality_co, airkorea_air_quality_co_grade, "
            "airkorea_air_quality_o3, airkorea_air_quality_o3_grade, "
            "airkorea_air_quality_no2, airkorea_air_quality_no2_grade, "
            "airkorea_air_quality_pm10, airkorea_air_quality_pm10_grade, airkorea_air_quality_pm10_forecast, "
            "airkorea_air_quality_pm25, airkorea_air_quality_pm25_grade, airkorea_air_quality_pm25_forecast, "
            "airkorea_air_quality_khai, airkorea_air_quality_khai_grade, airkorea_station_name) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_kw_dust():
    """ Kweather에서 미세먼지 정보 데이터 API 호출
        Args:

        Returns:
            json_data (list): Kweather의 미세먼지 정보 데이터
    """
    response = requests.get(KWEATHER_HOST)
    json_data = response.json()[0]['findust']['station']
    return json_data


@app.task(bind=True, queue='main', expires=60)
def insert_kw_dust(self):
    """ Kweather에서 미세먼지 정보 데이터를 받아와 Key로 Database에 Insert
        Args:

        Returns:
            json_data (list): Kweather의 미세먼지 정보 데이터
    """
    conn = psql_connect()
    curs = conn.cursor()
    for dust_list in call_kw_dust():
        values_data = (dust_list['areaName_wide'], dust_list['areaName_city'], dust_list['areaName_dong'],
                       datetime.datetime.strptime(dust_list['announceTime'], '%Y%m%d%H%M'), dust_list['Lat'],
                       dust_list['Lng'], dust_list['PM10_VALUE'], dust_list['PM25_VALUE'])
        curs.execute(
            "INSERT INTO kweather_dust(kweather_dust_sd, kweather_dust_sgg, kweather_dust_emd, "
            "kweather_dust_announce_time, kweather_dust_lat, kweather_dust_lon, kweather_dust_pm100, "
            "kweather_dust_pm25) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", values_data)
    conn.commit()
    curs.close()
    conn.close()


def call_kw_dust_json():
    """ Kweather에서 미세먼지 정보 데이터 API 호출
        Args:

        Returns:
            json_data (dict): Kweather의 미세먼지 정보 데이터
    """
    response = requests.get(KWEATHER_HOST)
    json_data = response.json()[0]['findust']
    return json_data


@app.task(bind=True, queue='main', expires=60)
def insert_kw_dust_json(self):
    """ Kweather에서 미세먼지 정보 데이터를 받아와 JSON째로 Database에 Insert
        Args:

        Returns:
            json_data (list): Kweather의 미세먼지 정보 데이터
    """
    logger.debug('insert_kw_dust_json 실행')
    json_data = call_kw_dust_json()
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
        print("db_time : " + db_time + ", kw_time : " + kw_time)
        null_kw_dust_json.apply_async(countdown=(60), queue='sub', expires=70)
        return
    values_data = (json.dumps(json_data, ensure_ascii=False),)
    curs = conn.cursor()
    curs.execute("INSERT INTO kweather_dust_json(kweather_dust_json_data) VALUES(%s)", values_data)
    conn.commit()
    curs.close()
    conn.close()


@app.task(bind=True, queue='sub', max_retries=3, time_limit=70)
def null_kw_dust_json(self):
    logger.debug('null_kw_dust_json 실행')
    json_data = call_kw_dust_json()
    conn = psql_connect()
    select_time_sql = "SELECT kweather_dust_json_data -> 'station' -> 1 ->> 'announceTime' AS announceTime " \
                      "FROM kweather_dust_json ORDER BY kweather_dust_json_date DESC LIMIT 1"
    db_time = psql_select(conn, select_time_sql)[0][0]
    kw_time = json_data['station'][0]['announceTime']
    if db_time == kw_time:
        print("db_time : " + db_time + ", kw_time : " + kw_time)
        self.retry(countdown=(60), max_retries=2, queue='sub', time_limit=70)
        return
    values_data = (json.dumps(json_data, ensure_ascii=False),)
    curs = conn.cursor()
    curs.execute("INSERT INTO kweather_dust_json(kweather_dust_json_data) VALUES(%s)", values_data)
    conn.commit()
    curs.close()
    conn.close()

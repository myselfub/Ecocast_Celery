import requests
from psycopg2.pool import ThreadedConnectionPool
import psycopg2
import json
from celery import Celery
import datetime
from datetime import timedelta
from celery.schedules import crontab
import logging
import os
from time import sleep

"""
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

# os.system('celery beat -A api_data -l debug')

if not (os.path.isdir('log')):
    os.mkdir(os.path.join('log'))

logger = logging.getLogger(__name__)
log_format = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
file_max_bytes = 10 * 1024 * 1024
log_file_handler = logging.handlers.RotatingFileHandler('./log/api_data_celery_log.log', maxBytes=file_max_bytes,
                                                        backupCount=20)
log_file_handler.setFormatter(log_format)
logger.addHandler(log_file_handler)
logger.setLevel(level=logging.DEBUG)

AIRKOREA_HOST = 'http://openapi.airkorea.or.kr/openapi/services/rest/'
AIRKOREA_KEY = 'AIRKOREA_KEY'  # URL Decoded
KWEATHER_HOST = 'http://hosting.kweather.co.kr/bigdata/digital_air/digital_air_json.php?api_key=KWEATHER_KEY'
DB_HOST = 'DB_HOST'
DB_NAME = 'DB_NAME'
DB_USER = 'DB_USER'
DB_PW = 'DB_PW'
DB_PORT = 'DB_PORT'
DB_POOL = ThreadedConnectionPool(1, 25, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)


def psql_connect():
    """ PostgreSQL Connection Pool
        Args:

        Returns:
            conn (object): connection Object
    """
    if (DB_POOL):
        return DB_POOL.getconn()
    else:
        return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)


def psql_select(conn, sql):
    """ PostgreSQL Connection Pool
        Args:
            conn (object): connection Object
            sql (str): SQL Querry
        Returns:
            curs.fetchall() (object): Result Data due to SELECT
    """
    curs = conn.cursor()
    curs.execute(sql)
    return curs.fetchall()


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
        'insert_ak_air_quality_list-hour': {
            'task': 'api_data.insert_ak_air_quality_list',
            'schedule': timedelta(hours=1),
            'args': ()
        },
        # 'insert_kw_dust-10minute': {
        #     'task': 'api_data.insert_kw_dust',
        #     'schedule': timedelta(minutes=10),
        #     'args': ()
        # },
        'insert_kw_dust_json-minute': {
            'task': 'api_data.insert_kw_dust_json',
            'schedule': timedelta(minutes=5),
            'args': ()
        },
        # 'insert_kw_dust_json-crontab': {
        #     'task': 'api_data.insert_kw_dust_json_crontab',
        #     'schedule': crontab(hour='*', minute='*'),
        #     'args': ()
        # },
    }
)


def call_ak_station():
    """ Airkorea의 측정소 정보 데이터 API 호출
        Args:

        Returns:
            jsonData['list'] (list): Airkorea의 측정소 정보 데이터
    """
    conn = psql_connect()
    curs = conn.cursor()
    url = AIRKOREA_HOST + 'MsrstnInfoInqireSvc/getMsrstnList'
    param = {}
    param['serviceKey'] = AIRKOREA_KEY
    param['numOfRows'] = 600
    param['pageNo'] = 1
    param['_returnType'] = 'json'
    response = requests.get(url, param)
    jsonData = response.json()  # jsonData = json.loads(response.text)
    return jsonData['list']


@app.task(bind=True, queue='main')
def insert_ak_station(self):
    """ Airkorea의 측정소 정보 데이터를 받아와 Database에 Insert
        Args:

        Returns:
    """
    logger.debug('insert_ak_station 실행')
    conn = psql_connect()
    curs = conn.cursor()
    for list in call_ak_station():
        if list['dmX'] is "":
            list['dmX'] = -1
        if list['dmY'] is "":
            list['dmY'] = -1
        if list['year'] is "":
            list['year'] = 0
        valuesData = (list['addr'], list['dmX'], list['dmY'], list['item'], list['mangName'], list['map'], list['oper'],
                      list['photo'],
                      list['stationName'], list['year'])
        curs.execute(
            "INSERT INTO airkorea_station(airkorea_station_address, airkorea_station_lat, airkorea_station_lon, "
            "airkorea_station_item, airkorea_station_mang, airkorea_station_map, airkorea_station_oper, "
            "airkorea_station_photo, airkorea_station_name, airkorea_station_year) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", valuesData)
    conn.commit()
    curs.close()
    DB_POOL.putconn(conn)
    logger.debug('insert_ak_station 종료')


@app.task(bind=True, queue='main')
def insert_ak_air_quality_list():
    """ Airkorea의 측정소 정보 데이터 중 API로 호출될 측정소명 SELECT
        Args:

        Returns:
    """
    logger.debug('insert_ak_air_quality_list 실행')
    conn = psql_connect()
    sql = 'SELECT airkorea_station_name FROM airkorea_station WHERE airkorea_station_is_active = TRUE'
    result = psql_select(conn, sql)
    DB_POOL.putconn(conn)
    for station in result:
        insert_ak_air_quality.apply_async(args=[station], queue='main', expires=60)
    logger.debug('insert_ak_air_quality_list 종료')


def call_ak_air_quality(stationName):
    """ Airkorea의 공기질 정보 데이터 API 호출
        Args:
            stationName (str): Airkorea에서 제공하는 측정소 정보 데이터의 측정소명

        Returns:
            jsonData['list'] (list): Airkorea의 공기질 정보 데이터
    """
    logger.debug('call_ak_air_quality(' + stationName + ') 실행')
    url = AIRKOREA_HOST + 'ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
    param = {}
    param['serviceKey'] = AIRKOREA_KEY
    # param['numOfRows'] = 30
    param['numOfRows'] = 1
    param['pageNo'] = 1
    param['stationName'] = stationName
    param['dataTerm'] = 'DAILY'
    param['ver'] = 1.3
    param['_returnType'] = 'json'
    response = requests.get(url, param)
    jsonData = response.json()
    logger.debug('call_ak_air_quality(' + stationName + ') 종료')
    return jsonData['list']


@app.task(bind=True, queue='main')
def insert_ak_air_quality(self, stationName):
    """ Airkorea의 공기질 정보 데이터를 받아와 Database에 Insert
        Args:
            stationName (str): Airkorea에서 제공하는 측정소 정보 데이터의 측정소명

        Returns:
    """
    logger.debug('insert_ak_air_quality(' + stationName + ') 실행')
    air_quality_data = call_ak_air_quality(stationName)[0]
    values_data = (
        air_quality_data['dataTime'], air_quality_data['mangName'], air_quality_data['so2Value'],
        air_quality_data['so2Grade'],
        air_quality_data['coValue'], air_quality_data['coGrade'], air_quality_data['o3Value'],
        air_quality_data['o3Grade'],
        air_quality_data['no2Value'], air_quality_data['no2Grade'], air_quality_data['pm10Value'],
        air_quality_data['pm10Grade'],
        air_quality_data['pm10Value24'], air_quality_data['pm25Value'], air_quality_data['pm25Grade'],
        air_quality_data['pm25Value24'],
        air_quality_data['khaiValue'], air_quality_data['khaiGrade'], stationName)
    conn = psql_connect()
    curs = conn.cursor()
    curs.execute(
        "INSERT INTO airkorea_air_quality(airkorea_air_quality_time, airkorea_air_quality_mang, airkorea_air_quality_so2, airkorea_air_quality_so2_grade, "
        "airkorea_air_quality_co, airkorea_air_quality_co_grade, airkorea_air_quality_o3, airkorea_air_quality_o3_grade, airkorea_air_quality_no2, airkorea_air_quality_no2_grade, "
        "airkorea_air_quality_pm10, airkorea_air_quality_pm10_grade, airkorea_air_quality_pm10_forecast, airkorea_air_quality_pm25, airkorea_air_quality_pm25_grade, "
        "airkorea_air_quality_pm25_forecast, airkorea_air_quality_khai, airkorea_air_quality_khai_grade, airkorea_station_name) "
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", values_data)
    conn.commit()
    curs.close()
    DB_POOL.putconn(conn)
    for value_data in values_data:
        if value_data == '' or value_data == '-':
            null_ak_air_quality.apply_async(queue='sub', expires=60)
            break
    logger.debug('insert_ak_air_quality(' + stationName + ') 종료')


@app.task(bind=True, queue='sub')
def null_ak_air_quality(self, stationName):
    """ Airkorea에서 받아온 공기질 정보 데이터가 null 값이 들어가 있을때
        Args:
            stationName (str): Airkorea에서 제공하는 측정소 정보 데이터의 측정소명

        Returns:
    """
    logger.debug('null_ak_air_quality(' + stationName + ') 실행')
    air_quality_data = call_ak_air_quality(stationName)[0]
    values_data = (
        air_quality_data['so2Value'], air_quality_data['so2Grade'],
        air_quality_data['coValue'], air_quality_data['coGrade'], air_quality_data['o3Value'],
        air_quality_data['o3Grade'],
        air_quality_data['no2Value'], air_quality_data['no2Grade'], air_quality_data['pm10Value'],
        air_quality_data['pm10Grade'],
        air_quality_data['pm10Value24'], air_quality_data['pm25Value'], air_quality_data['pm25Grade'],
        air_quality_data['pm25Value24'],
        air_quality_data['khaiValue'], air_quality_data['khaiGrade'], air_quality_data['dataTime'], stationName)
    for value_data in values_data:
        if value_data == '' or value_data == '-':
            self.retry(countdown=(3 * 60), max_retries=15, queue='sub', time_limit=60)
            return
    conn = psql_connect()
    curs = conn.cursor()
    curs.execute(
        "UPDATE airkorea_air_quality SET airkorea_air_quality_so2 = %s, airkorea_air_quality_so2_grade = %s, "
        "airkorea_air_quality_co = %s, airkorea_air_quality_co_grade = %s, airkorea_air_quality_o3 = %s, airkorea_air_quality_o3_grade, = %s airkorea_air_quality_no2 = %s, airkorea_air_quality_no2_grade = %s, "
        "airkorea_air_quality_pm10 = %s, airkorea_air_quality_pm10_grade = %s, airkorea_air_quality_pm10_forecast = %s, airkorea_air_quality_pm25 = %s, airkorea_air_quality_pm25_grade = %s, "
        "airkorea_air_quality_pm25_forecast = %s, airkorea_air_quality_khai = %s, airkorea_air_quality_khai_grade = %s "
        "WHERE airkorea_air_quality_time = %s AND airkorea_station_name = %s", values_data)
    conn.commit()
    curs.close()
    DB_POOL.putconn(conn)
    logger.debug('null_ak_air_quality(' + stationName + ') 종료')


@app.task(bind=True, queue='main')
def insert_ak_air_quality_all(self, stationName):
    conn = psql_connect()
    curs = conn.cursor()
    for list in call_ak_air_quality(stationName):
        valuesData = (
            list['dataTime'], list['mangName'], list['so2Value'], list['so2Grade'], list['coValue'], list['coGrade'],
            list['o3Value'], list['o3Grade'], list['no2Value'], list['no2Grade'], list['pm10Value'], list['pm10Grade'],
            list['pm10Value24'], list['pm25Value'], list['pm25Grade'], list['pm25Value24'], list['khaiValue'],
            list['khaiGrade'], stationName)
        curs.execute(
            "INSERT INTO airkorea_air_quality(airkorea_air_quality_time, airkorea_air_quality_mang, airkorea_air_quality_so2, airkorea_air_quality_so2_grade, "
            "airkorea_air_quality_co, airkorea_air_quality_co_grade, airkorea_air_quality_o3, airkorea_air_quality_o3_grade, airkorea_air_quality_no2, airkorea_air_quality_no2_grade, "
            "airkorea_air_quality_pm10, airkorea_air_quality_pm10_grade, airkorea_air_quality_pm10_forecast, airkorea_air_quality_pm25, airkorea_air_quality_pm25_grade, "
            "airkorea_air_quality_pm25_forecast, airkorea_air_quality_khai, airkorea_air_quality_khai_grade, airkorea_station_name) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", valuesData)
    conn.commit()
    curs.close()
    DB_POOL.putconn(conn)


def call_kw_dust():
    """ Kweather에서 미세먼지 정보 데이터 API 호출
        Args:

        Returns:
            jsonData (list): Kweather의 미세먼지 정보 데이터
    """
    response = requests.get(KWEATHER_HOST)
    jsonData = response.json()[0]['findust']['station']
    return jsonData


@app.task(bind=True, queue='main')
def insert_kw_dust(self):
    """ Kweather에서 미세먼지 정보 데이터를 받아와 Key로 Database에 Insert
        Args:

        Returns:
            jsonData (list): Kweather의 미세먼지 정보 데이터
    """
    conn = psql_connect()
    curs = conn.cursor()
    for list in call_kw_dust():
        valuesData = (list['areaName_wide'], list['areaName_city'], list['areaName_dong'],
                      datetime.datetime.strptime(list['announceTime'], '%Y%m%d%H%M'), list['Lat'], list['Lng'],
                      list['PM10_VALUE'], list['PM25_VALUE'])
        curs.execute(
            "INSERT INTO kweather_dust(kweather_dust_sd, kweather_dust_sgg, kweather_dust_emd, kweather_dust_announce_time, "
            "kweather_dust_lat, kweather_dust_lon, kweather_dust_pm100, kweather_dust_pm25) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", valuesData)
    conn.commit()
    curs.close()
    DB_POOL.putconn(conn)


def call_kw_dust_json():
    """ Kweather에서 미세먼지 정보 데이터 API 호출
        Args:

        Returns:
            jsonData (dict): Kweather의 미세먼지 정보 데이터
    """
    response = requests.get(KWEATHER_HOST)
    jsonData = response.json()[0]['findust']
    return jsonData


@app.task(bind=True, queue='main')
def insert_kw_dust_json(self):
    """ Kweather에서 미세먼지 정보 데이터를 받아와 JSON째로 Database에 Insert
        Args:

        Returns:
            jsonData (list): Kweather의 미세먼지 정보 데이터
    """
    logger.debug('insert_kw_dust_json 실행')
    valuesData = (json.dumps(call_kw_dust_json(), ensure_ascii=False),)
    conn = psql_connect()
    curs = conn.cursor()
    curs.execute("INSERT INTO kweather_dust_json(kweather_dust_json_data) VALUES(%s);", valuesData)
    conn.commit()
    curs.close()
    DB_POOL.putconn(conn)
    logger.debug('insert_kw_dust_json 종료')

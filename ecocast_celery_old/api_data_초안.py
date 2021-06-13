import requests
import psycopg2
import datetime
import json

AIRKOREA_HOST = 'http://openapi.airkorea.or.kr/openapi/services/rest/'
# 공공데이터포털에서 제공하는 인증키는 URL인코딩 상태의 키를 제공, URL인코딩된 키를 다시 인코딩 하기 때문에 디코딩된 데이터로 바꿔서 넣어야됨
AIRKOREA_KEY = 'AIRKOREA_KEY'
KWEATHER_HOST = 'http://hosting.kweather.co.kr/bigdata/digital_air/digital_air_json.php?api_key=KWEATHER_KEY'
DB_HOST = 'localhost'
DB_NAME = 'DB_NAME'
DB_USER = 'DB_USER'
DB_PW = 'DB_PW'
DB_PORT = 'DB_PORT'


def psql_connect():
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)
    return conn


def psql_select(conn, sql):
    curs = conn.cursor()
    curs.execute(sql)
    # result = curs.fetchall()
    # print(result)
    return curs


def call_ak_station():
    url = AIRKOREA_HOST + 'MsrstnInfoInqireSvc/getMsrstnList'
    param = {}
    param['serviceKey'] = AIRKOREA_KEY
    param['numOfRows'] = 600
    param['pageNo'] = 1
    param['_returnType'] = 'json'
    response = requests.get(url, param)
    jsonData = response.json()  # jsonData = json.loads(response.text)
    return jsonData['list']


def insert_ak_station():
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
            "INSERT INTO airkorea_measurement(airkorea_measurement_address, airkorea_measurement_lat, airkorea_measurement_lon, "
            "airkorea_measurement_item, airkorea_measurement_mang, airkorea_measurement_map, airkorea_measurement_oper, "
            "airkorea_measurement_photo, airkorea_measurement_station, airkorea_measurement_year) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", valuesData)
    conn.commit()
    curs.close()
    conn.close()


def call_ak_dust(stationName):
    url = AIRKOREA_HOST + 'ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
    param = {}
    param['serviceKey'] = AIRKOREA_KEY
    param['numOfRows'] = 30
    param['pageNo'] = 1
    param['stationName'] = stationName
    param['dataTerm'] = 'DAILY'
    param['ver'] = 1.3
    param['_returnType'] = 'json'
    response = requests.get(url, param)
    jsonData = response.json()
    return jsonData['list']


def insert_ak_dust(stationName):
    conn = psql_connect()
    curs = conn.cursor()
    for list in call_ak_dust(stationName):
        valuesData = (
            list['dataTime'], list['mangName'], list['so2Value'], list['coValue'], list['o3Value'], list['no2Value'],
            list['pm10Value'],
            list['pm10Value24'], list['pm25Value'], list['pm25Value24'], list['khaiValue'], stationName)
        curs.execute(
            "INSERT INTO airkorea_dust(airkorea_dust_announce_time, airkorea_dust_mang, airkorea_dust_so2, airkorea_dust_co, "
            "airkorea_dust_o3, airkorea_dust_no2, airkorea_dust_pm10, airkorea_dust_pm10_forecast, airkorea_dust_pm25, "
            "airkorea_dust_pm25_forecast, airkorea_dust_khai, airkorea_dust_station_name) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", valuesData)
    conn.commit()
    curs.close()
    conn.close()


def call_kw_dust():
    response = requests.get(KWEATHER_HOST)
    jsonData = response.json()[0]['findust']['station']
    return jsonData


def insert_kw_dust():
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
    conn.close()


def call_kw_dust_json():
    response = requests.get(KWEATHER_HOST)
    jsonData = response.json()[0]['findust']
    return jsonData


def insert_kw_dust_json():
    conn = psql_connect()
    curs = conn.cursor()
    valuesData = (json.dumps(call_kw_dust_json()),)
    curs.execute("INSERT INTO kweather_dust_json(kweather_dust_json_data) "
                 "VALUES(%s);", valuesData)
    conn.commit()
    curs.close()
    conn.close()


insert_kw_dust_json()

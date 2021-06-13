from celery import Celery
from celery.result import allow_join_result
from tasks import ak_station_task, ak_air_quality_task, kw_dust_task, ow_weather_task, data_weather_task
from time import sleep
import random, datetime

""" Celery Main Module
    Version: 1.0
    Author: Ecoplay 김유빈
    Date: 2020.10.16
    Comment:
        1. Airkorea Data: 1시간 주기, Kweather Data: 5분 주기, Public Data Porter
        2. 공공데이터포털에서 제공하는 인증키(AIRKOREA_KEY)는 URL인코딩 상태의 키를 제공, 
        URL인코딩된 키를 다시 인코딩 하기 때문에 디코딩된 데이터로 바꿔서 넣어야됩니다.
"""

app = Celery('ecocast_celery', backend='rpc://', broker='redis://')


@app.task(bind=True, queue='sub', max_retries=3, expires=300)
def insert_ak_station(self):
    if ak_station_task.insert_ak_station() is True:
        self.retry(countdown=(25), queue='sub', max_retries=2, expires=300)


@app.task(bind=True, queue='ak', expires=1500)
def insert_ak_air_quality_list(self):
    with allow_join_result():
        station_result = ak_air_quality_task.insert_ak_air_quality_list()
    if station_result is True:
        self.retry(countdown=(3 * 60), max_retries=15, queue='ak', time_limit=1500)
    else:
        for station in station_result:
            insert_ak_air_quality.apply_async(args=[station[0]], queue='ak', expires=600)
            sleep(random.randint(11, 20))


@app.task(bind=True, queue='ak', max_retries=3, expires=600)
def insert_ak_air_quality(self, station_name):
    with allow_join_result():
        insert_result = ak_air_quality_task.insert_ak_air_quality(station_name)
    if insert_result is True:
        null_ak_air_quality.apply_async(countdown=(2 * 60), args=[station_name], max_retries=2, queue='sub',
                                        expires=600)
    elif insert_result == 'Retry':
        self.retry(countdown=(25), args=[station_name], queue='ak', expires=600)


@app.task(bind=True, queue='sub', max_retries=10, time_limit=600)
def null_ak_air_quality(self, station_name):
    with allow_join_result():
        null_result = ak_air_quality_task.null_ak_air_quality(station_name)
    if null_result is True:
        self.retry(countdown=(2 * 60), max_retries=9, queue='sub', time_limit=600)


@app.task(bind=True, queue='sub', max_retries=3, expires=240)
def insert_kw_dust(self):
    if kw_dust_task.insert_kw_dust() is True:
        self.retry(countdown=(25), queue='sub', max_retries=2, expires=240)


@app.task(bind=True, queue='sub', expires=240)
def insert_kw_dust_json(self):
    if kw_dust_task.insert_kw_dust_json() is True:
        null_kw_dust_json.apply_async(countdown=(60), queue='sub', expires=240)


@app.task(bind=True, queue='sub', max_retries=3, time_limit=240)
def null_kw_dust_json(self):
    if kw_dust_task.null_kw_dust_json() is True:
        self.retry(countdown=(60), max_retries=2, queue='sub', time_limit=240)


@app.task(bind=True, queue='sub', max_retries=3, time_limit=240)
def insert_ow_weather_json(self, lat: float, lon: float):
    if ow_weather_task.insert_ow_weather_json(lat, lon) is True:
        self.retry(countdown=(25), max_retries=2, queue='sub', time_limit=240)


@app.task(bind=True, queue='data', max_retries=10, expires=1500)
def insert_data_weather_area_list_1_hour(self):
    with allow_join_result():
        area_result = data_weather_task.insert_data_weather_area_list()
    if area_result is True:
        self.retry(countdown=(2 * 60), max_retries=9, queue='data', time_limit=1500)
    else:
        now = datetime.datetime.now()
        # now = datetime.datetime.strptime(str(int(datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M')) - 100), '%Y%m%d%H%M')
        for area in area_result:
            insert_data_weather_1_hour.apply_async(args=[area['data_weather_area_x'], area['data_weather_area_y'], now],
                                                   queue='data', expires=600)
            sleep(random.randint(2, 4))


@app.task(bind=True, queue='data', max_retries=3, expires=600)
def insert_data_weather_1_hour(self, x: int, y: int, nows: str):
    now = datetime.datetime.strptime(nows, '%Y-%m-%dT%H:%M:%S.%f')
    # now = datetime.datetime.strptime(nows, '%Y-%m-%dT%H:%M:%S')
    if data_weather_task.insert_data_weather_1_hour(int(x), int(y), now) is True:
        self.retry(countdown=(25), max_retries=2, queue='data', time_limit=600)


@app.task(bind=True, queue='data', max_retries=10, expires=1500)
def insert_data_weather_area_list_3_hour(self):
    with allow_join_result():
        area_result = data_weather_task.insert_data_weather_area_list()
    if area_result is True:
        self.retry(countdown=(2 * 60), max_retries=9, queue='data', time_limit=1500)
    else:
        now = datetime.datetime.now()
        for area in area_result:
            insert_data_weather_3_hour.apply_async(args=[area['data_weather_area_x'], area['data_weather_area_y'], now],
                                                   queue='data', expires=600)
            sleep(random.randint(2, 4))


@app.task(bind=True, queue='data', max_retries=3, expires=600)
def insert_data_weather_3_hour(self, x: int, y: int, nows: str):
    now = datetime.datetime.strptime(nows, '%Y-%m-%dT%H:%M:%S.%f')
    if data_weather_task.insert_data_weather_3_hour(int(x), int(y), now) is True:
        self.retry(countdown=(25), max_retries=2, queue='data', time_limit=600)

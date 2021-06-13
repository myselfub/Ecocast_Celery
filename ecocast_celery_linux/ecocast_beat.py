from celery.schedules import crontab
from ecocast_app import app
from datetime import timedelta

""" Celery Beat 설정 모듈 """

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
            'task': 'ecocast_app.insert_ak_air_quality_list',
            'schedule': crontab(minute='1', hour='*'),  # timedelta(hours=1),
            'args': ()
        },
        'insert_kw_dust_json-5-minute': {
            'task': 'ecocast_app.insert_kw_dust_json',
            'schedule': crontab(minute='1,6,11,16,21,26,31,36,41,46,51,56'),  # timedelta(minutes=5),
            'args': ()
        },
        'insert_data_weather_list-1-hour': {
            'task': 'ecocast_app.insert_data_weather_area_list_1_hour',
            'schedule': crontab(minute='46', hour='*'),
            'args': ()
        },
        'insert_data_weather_list-3-hour': {
            'task': 'ecocast_app.insert_data_weather_area_list_3_hour',
            'schedule': crontab(minute='11', hour='2,5,8,11,14,17,20,23'),
            'args': ()
        },
        # 'insert_ak_station-once': {
        #     'task': 'ecocast_app.insert_ak_station',
        #     'schedule': timedelta(seconds=5),
        #     'args': ()
        # },
        # 'insert_ow_weather_json-1-hour': {
        #     'task': 'ecocast_app.insert_ow_weather_json',
        #     'schedule': crontab(minute='1', hour='*'),
        #     'args': ()
        # },
    }
)

from psycopg2.pool import ThreadedConnectionPool
import psycopg2
import psycopg2.extras
from psycopg2.extensions import new_type, DECIMAL
import logging.handlers
import json

""" Celery Main Module
    Version: 1.0
    Author: Ecoplay 김유빈
    Date: 2020.10.16
    Comment:
        1. Airkorea Data: 1시간 주기, Kweather Data: 5분 주기, Public Data Portal: 1시간, 3시간 주기
        2. 공공데이터포털에서 제공하는 인증키(AIRKOREA_KEY)는 URL인코딩 상태의 키를 제공, 
        URL인코딩된 키를 다시 인코딩 하기 때문에 디코딩된 데이터로 바꿔서 넣어야됩니다.
"""

# os.system('celery beat -A api_data -l DEBUG')

# if not (os.path.isdir('log')):
#     os.mkdir(os.path.join('log'))

logger = logging.getLogger(__name__)
log_format = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
file_max_bytes = 10 * 1024 * 1024
file_name_format = '%Y%m%d_'
log_file_handler = logging.handlers.TimedRotatingFileHandler(
    filename='./ecocast_celery/tasks/log/ecocast_celery_log.log',
    when='midnight',
    interval=1,
    backupCount=20, encoding='utf-8')
log_file_handler.prefix = file_name_format
log_file_handler.setFormatter(log_format)
logger.propagate = False
logger.addHandler(log_file_handler)
logger.setLevel(level=logging.DEBUG)

AIRKOREA_HOST = 'http://apis.data.go.kr/B552584/'
AIRKOREA_KEY = 'AIRKOREA_KEY'  # URL Decoded
KWEATHER_HOST = 'http://hosting.kweather.co.kr/bigdata/digital_air/digital_air_json.php?' \
                'api_key=KWEATHER_KEY'
OPENWEATHER_HOST = 'https://api.openweathermap.org/data/2.5/onecall'
OPENWEATHER_KEY = 'OPENWEATHER_KEY'
DATA_WEATHER_HOST = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/'
DATA_WEATHER_KEY = 'DATA_WEATHER_KEY'  # URL Decoded
REQUEST_CONNECT = 10
REQUEST_READ = 15
REQUEST_TIME_OUT = (REQUEST_CONNECT, REQUEST_READ)
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
    DEC_2_FLOAT = new_type(DECIMAL.values, 'DEC2FLOAT', lambda value, curs: float(value) if value is not None else None)
    psycopg2.extensions.register_type(DEC_2_FLOAT)
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)
    if DB_POOL:
        return DB_POOL.getconn()
    else:
        return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PW, port=DB_PORT)


def psql_select(conn, sql):
    """ PostgreSQL Select
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


def psql_dict_select(conn, sql):
    """ PostgreSQL Select To JSON
        Args:
            conn (object): connection Object
            sql (str): SQL Query
        Returns:
            curs.fetchall() (object): Result Data due to SELECT
    """
    curs = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    curs.execute(sql)
    result = json.loads(json.dumps(curs.fetchall(), ensure_ascii=False))
    curs.close()
    return result

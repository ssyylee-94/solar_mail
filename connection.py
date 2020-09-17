from sqlalchemy import create_engine
import settings


def getmysql(user, password, url_add, db_name):

    engine_ = create_engine("mysql+pymysql://" + user + ":" + password + "@" + url_add + "/" + db_name, encoding='utf-8')
    conn_ = engine_.connect()

    return engine_, conn_


class SolarDB:

    def __init__(self):
        engine, conn = getmysql(settings.user_id, settings.pw, settings.url, settings.name1)

        self.engine = engine
        self.conn = conn

    def engine(self):
        return self.engine

    def conn(self):
        return self.conn


class KmaDB:
    def __init__(self):
        engine, conn = getmysql(settings.user_id, settings.pw, settings.url, settings.name2)

        self.engine = engine
        self.conn = conn

    def engine(self):
        return self.engine

    def conn(self):
        return self.conn

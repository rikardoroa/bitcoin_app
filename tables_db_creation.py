import logging.config
import sys
from dotenv import load_dotenv
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import ConnectionException
import psycopg2
import configparser

load_dotenv()
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('tracer')
config = configparser.ConfigParser()
config.read('queries.ini')
config.sections()


class db_tables:

    def __init__(self):
        self.user = os.getenv('user')
        self.passw = os.getenv('pass')
        self.host = os.getenv('host')
        self.conn = psycopg2.connect(database='postgres', user=self.user, password=self.passw, host=self.host,
                                     port='5432')
        self.query1 = config.get('main', 'q1')
        self.query2 = config.get('main', 'q2')
        self.query5 = config.get('main', 'q5')


    def creating_tables(self):
        try:

            print("----please wait a moment..verifying or creating databases and tables..---")
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = self.conn.cursor()
            cursor.execute("CREATE DATABASE coins_detail")
            cursor.close()
            self.conn.close()
            conn = psycopg2.connect(database='coins_detail', user=self.user, password=self.passw, host=self.host,
                                    port='5432')
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor_ = conn.cursor()
            cursor_.execute(self.query1)
            cursor_.execute(self.query2)
            cursor_.close()
            conn.close()
        except Exception:
            logger.warning("Database and table already created!")
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
            raise sys.exit("CTRL- C PRESSED!")
        except ConnectionException:
            logger.error("Could no connect to the db, please review connection")
        finally:
            logger.debug("Databases as tables created successfully!")

    def average_price(self):
        try:
            conn = psycopg2.connect(database='coins_detail', user=self.user, password=self.passw, host=self.host,
                                    port='5432')
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor_ = conn.cursor()
            cursor_.execute(self.query5)
            print(cursor_.fetchall())
            cursor_.close()
            conn.close()
        except ConnectionException:
            logger.error("Could no connect to the db, please review connection")
            raise sys.exit("can't connect db, review connections an try again!")


# script by rikardoroa
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
        self.conn_db2 = None
        self.conn_db1 = psycopg2.connect(database='postgres', user=os.getenv('user'),
                                         password=os.getenv('pass'), host=os.getenv('host'), port='5432')
        self.query1 = config.get('main', 'q1')
        self.query2 = config.get('main', 'q2')
        self.query5 = config.get('main', 'q5')

    def database_creation(self):
        """ this function creates a database named COINS_DETAIL """
        try:
            print("----please wait a moment..verifying or creating databases and tables..---")
            self.conn_db1.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = self.conn_db1.cursor()
            cursor.execute("CREATE DATABASE coins_detail")
            cursor.close()
            self.conn_db1.close()
        except Exception:
            logger.warning("Database and table already created!")
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
        except ConnectionException:
            logger.error("Could no connect to the db, please review connection")
        finally:
            logger.debug("Databases as tables created successfully!")

    def tables_creation(self):
        """ this function creates two tables, named COINS_DETAIL
            and COINS_DETAIL_PRICE where the data will be uploaded
            into the database COINS_DETAIL
        """
        try:
            self.conn_db2 = psycopg2.connect(database='coins_detail', user=os.getenv('user'), password=os.getenv('pass'),
                                             host=os.getenv('host'), port='5432')
            self.conn_db2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor_ = self.conn_db2.cursor()
            cursor_.execute(self.query1)
            cursor_.execute(self.query2)
            cursor_.close()
            return self.conn_db2
        except Exception:
            logger.warning("Database and table already created!")
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
        except ConnectionException:
            logger.error("Could no connect to the db, please review connection")
        finally:
            logger.debug("Databases as tables created successfully!")

    def average_price_per_coin(self):
        """function that calls a query to insert the price average
           per coin and per month on the coins_price_detail table
        """
        try:
            self.conn_db2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor_ = self.conn_db2.cursor()
            cursor_.execute(self.query5)
            print(cursor_.fetchall())
            cursor_.close()
            self.conn_db2.close()
        except ConnectionException:
            logger.error("Could no connect to the db, please review connection")



if __name__ == "__main__":
    db = db_tables()
    db.database_creation()
    db.tables_creation()

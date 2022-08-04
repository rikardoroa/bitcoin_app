import logging.config
import json
import sys
from dotenv import load_dotenv
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import ConnectionException
import psycopg2

load_dotenv()
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('tracer')


class db_tables:
    
    #init variables
    def __init__(self):
        self.user = os.getenv('user')
        self.passw = os.getenv('pass')
        self.host = os.getenv('host')
        self.db = os.getenv('db')
        self.path = os.path.abspath("local_queries.json")
        self.conn = psycopg2.connect(database=self.db, user=self.user, password=self.passw, host=self.host,
                                     port='5432')

    #db creation
    def creating_tables(self):
        try:
            query1 = ""
            query2 = ""
            print("----please wait a moment..verifying or creating databases and tables..---")
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = self.conn.cursor()
            cursor.execute("CREATE DATABASE coins_detail")
            cursor.close()
            self.conn.close()
            #load queries for tables creation
            with open(self.path, "r") as json_file:
                json_queries = json.load(json_file)
            for key in json_queries.keys():
                if "q1" in key:
                    query1 = json_queries[key]
                if "q2" in key:
                    query2 = json_queries[key]
            conn = psycopg2.connect(database='coins_detail', user=self.user, password=self.passw, host=self.host,
                                    port='5432')
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor_ = conn.cursor()
            cursor_.execute(query1)
            #cursor an connection closed
            cursor_.execute(query2)
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

        
    #calculating the usd average price
    def average_price(self):
        try:
            query5 = ""
            #load querie
            with open(self.path, "r") as json_file:
                json_queries = json.load(json_file)
            for key in json_queries.keys():
                if "q5" in key:
                    query5 = json_queries[key]
            #create connection
            conn = psycopg2.connect(database='coins_detail', user=self.user, password=self.passw, host=self.host,
                                    port='5432')
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor_ = conn.cursor()
            #showing results
            cursor_.execute(query5)
            print(cursor_.fetchall())
            cursor_.close()
            conn.close()
        except ConnectionException:
            logger.error("Could no connect to the db, please review connection")
            raise sys.exit("can't connect db, review connections an try again!")

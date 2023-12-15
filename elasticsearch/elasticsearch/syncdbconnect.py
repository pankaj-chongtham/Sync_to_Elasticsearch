import os
import sys
import pyodbc
import pymongo
import pymssql
import logging
import traceback
import configparser
from logging.handlers import RotatingFileHandler

# custom imports
import synclog

app_log = synclog.setup_logger()

if getattr(sys, 'frozen', False):
    # we are running in a bundle
    CURRENT_PATH = os.path.dirname(sys.executable)
else:
    # we are running in a normal Python environment
    CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
CURRENT_PATH = os.path.normpath(os.path.join(CURRENT_PATH, '..'))

# CONFIG FILE
config_filename = os.path.join(CURRENT_PATH, 'config.ini')
config_obj = configparser.ConfigParser()
config_obj.read(config_filename)

app_log.info("Configurations Loaded Successfully.")
app_log.info("DB Connection Module Loaded Successfully.")
app_log.info("currentpath: " + CURRENT_PATH)
app_log.info("configpath: " + config_filename)

def mongodb_database_connection(server, database):
    try:
        myclient = pymongo.MongoClient(server)
        mydb = myclient[database]

        return (mydb)
    except Exception as e:
        app_log.error("Error Occurred at: {} Data Obtained: {}".format(e, locals()))
        app_log.error(traceback.format_exc())


def pymssql_database_connection(server, database, username, password):
    try:
        db = pymssql.connect(host=server, user=username, password=password, database=database)
        cursor = db.cursor()

        return (cursor)
    except Exception as e:
        app_log.error("Error Occurred at: {} Data Obtained: {}".format(e, locals()))
        app_log.error(traceback.format_exc())


def pyodbc_database_connection(server, database, username, password):
    """
    SQL Server Connection using pyodbc
    Required ODBC Driver 17 for SQL Server install
    :param server: E.g. BAVNWASCNV02 or BAVNWASVN02.ramco,65328 or 192.168.0.1
    :param database: AVNAPPDB
    :param username: select
    :param password: select
    :return: pyodbc connection object
    """
    try:
        app_log.info("CONNECTING SQL DATABASE SERVER: {}, DATABASE: {}".format(server, database))
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        app_log.info('Connected.')
        return cnxn
    except Exception as e:
        app_log.exception('FAILED! Please Check details. [DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server +
                          ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password + ';Trusted_Connection=yes;')
        app_log.error("Error Occurred at: {} Data Obtained: {}".format(e, locals()))
        app_log.error(traceback.format_exc())
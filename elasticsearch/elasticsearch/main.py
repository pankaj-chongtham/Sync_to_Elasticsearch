# System Imports
import os
import sys
import time
import traceback
import functools
import numpy as np
import configparser
import pandas as pd
import concurrent.futures
from cryptography.fernet import Fernet
from elasticsearch import Elasticsearch

# Custom Imports
import synclog
import syncdbconnect

# ----------------------------------------------------------
#   ENVIRONMENT CONFIGURATION
# ----------------------------------------------------------
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

# LOG PATH CONFIGURATION
app_log = synclog.setup_logger()

# ELASTICSEARCH CONFIGURATION
es_url = config_obj['ELASTICSEARCH']['url']
es_base64 = config_obj['ELASTICSEARCH']['es_base64']

es_client = Elasticsearch(es_url, basic_auth=es_base64)
app_log.info(es_client.ping())

# SQL SERVER CONFIGURATION
db_server = config_obj['MSSQL']['server']
db_username = config_obj['MSSQL']['username']
db_password = config_obj['MSSQL']['password']
db_database = config_obj['MSSQL']['database']
db_connector = config_obj['MSSQL']['connector']

# DECODE SQL Server Password Key
with open(os.path.join(CURRENT_PATH, 'key.key'), 'rb') as r:
    KEY = r.read()
FERNET = Fernet(KEY)
db_password = FERNET.decrypt(db_password.encode('utf-8')).decode()

if db_connector == 'pyodbc':
    conn = syncdbconnect.pyodbc_database_connection(db_server, db_database, db_username, db_password)
    cursor = conn.cursor()
else:
    app_log.error('DB Connector Failed.')

# METADATA
parent_table = config_obj['METADATA']['parenttable']

# UTILITY SETTING
MAX_ROW = int(config_obj['UTILITYSETTING']['maxrow'])

REPLACER = {pd.NaT: None, np.nan: None}


# ----------------------------------------------------------
#   ENVIRONMENT CONFIGURATION
# ----------------------------------------------------------


def multiprocess(row, table, ids, parse_dates, query):
    app_log.info(f'Read from {row[0]} to {row[-1]}')
    ids = ", ".join([i for i in ids])
    app_log.info('Query: {}'.format(
        "SELECT {} FROM(SELECT *, ROW_NUMBER() OVER(ORDER BY {}) AS row FROM {} with(nolock)) TEMP WHERE row >= {} AND row < {}".format(
            query, ids, table, row[0], row[-1]
        )))
    df_temp = pd.read_sql(
        "SELECT {} FROM(SELECT *, ROW_NUMBER() OVER(ORDER BY {}) AS row FROM {} with(nolock)) TEMP WHERE row >= {} AND row < {}".format(
            query, ids, table, row[0], row[-1]
        ), con=conn, parse_dates=parse_dates).replace(REPLACER)
    return df_temp


def get_parse_dates(dbconnect, table):
    """
    Extracting Datatype from MSSQL Table using sp_help
    :param dbconnect: MSSQL Connection Object
    :param table: MSSQL Table Name
    :return: parse_dates: List of datetime datatype;
    timestamp: List of timestamp datatype; binary: List of vartimestamp datatype
    """
    # ---------------------------------------------------------------------------
    # GETTING DATATYPE DETAILS
    # ---------------------------------------------------------------------------
    result = dbconnect.engine.raw_connection().cursor().execute('sp_help "{}"'.format(table))
    result.nextset()
    data = result.fetchall()
    app_log.info('Getting list of Column')
    column = [column[0] for column in result.description]
    df = pd.DataFrame(eval(str(data)), columns=column)
    typelist = []
    item = 0
    app_log.info('Getting Datatype Details')
    while item < len(df['Type']):
        typelist.append(
            dbconnect.engine.raw_connection().cursor().execute('sp_help {}'.format(df['Type'][item])).fetchall()[0][1])
        item += 1
    df['datatype'] = typelist
    timestamp = [df['Column_name'][index] for index in df.loc[df['datatype'] == 'timestamp', ['Column_name']].index]
    binary = [df['Column_name'][index] for index in
              df.loc[df['datatype'].isin(['binary', 'varbinary']), ['Column_name']].index]
    parse_dates = [df['Column_name'][index] for index in
                   df.loc[df['datatype'] == 'datetime', ['Column_name']].index]
    return parse_dates, timestamp, binary


def multiprocess_df(table, row_count, ids, query):
    df = pd.DataFrame()
    app_log.info('Parsing Datetime and Timestamp datatype...')
    parse_dates, timestamp, binary = get_parse_dates(dbconnect=conn, table=table)
    if query != ['*']:
        parse_dates, timestamp, binary = list(set(query).intersection(set(parse_dates))), \
            list(set(query).intersection(set(timestamp))), \
            list(set(query).intersection(set(binary)))
    app_log.info('Getting unique_row')
    group_unique_row = [[i, i + MAX_ROW] for i in range(0, row_count + 1, MAX_ROW)]
    app_log.info('Multi-Process is initiated')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        pool = executor.map(functools.partial(multiprocess, table=table,
                                              ids=ids, parse_dates=parse_dates,
                                              query=query), group_unique_row)
        for res in pool:
            app_log.info('Concat each process df')
            df = pd.concat([df, res], axis=0)
            app_log.info(f'Total length of df {len(df)}')
    app_log.info('Completed merging df from all processes')
    # df[parse_dates] = df[parse_dates].astype(str)
    # CONVERTING TIMESTAMP FORMAT INTO HEX STRING
    app_log.info('Making Compatibility Timestamp dataype for {} table'.format(df))
    column = 0
    timestamp_binary = timestamp + binary
    while column < len(timestamp_binary):
        df[timestamp_binary[column]] = df[timestamp_binary[column]].apply(lambda v: v.hex())
        df[timestamp_binary[column]] = df[timestamp_binary[column]].astype(str)
        column += 1
    return df


def sync():
    start = time.perf_counter()
    # ----------------------------------------------------
    # GETTING TABLE DATA INTO DATAFRAME
    # ----------------------------------------------------
    parent_table_df = pd.read_sql('select * from {}'.format(parent_table), conn)
    for index, row in parent_table_df.iterrows():
        table_name = row['Elstc_table_name']
        elastic_index = row['Elstc_indx_name']
        parent_id = row['Elstc_search_id']
        push_query = row['Elstc_push_query']
        try:
            row_count = pd.read_sql(f"select count(*) from {table_name} with(nolock)", con=conn)
            row_count = row_count.loc[0].item()
            table_type = cursor.execute(f'sp_help {table_name}').fetchall()[0][2]
            if row_count > MAX_ROW and table_type == 'user table':
                parent_table_df = multiprocess_df(row_count=row_count, ids=parent_id, table=table_name,
                                                  query=push_query)
            elif row_count < MAX_ROW or table_type == 'view':
                # GETTING DATETIME AND TIMESTAMP DATATYPE FROM TABLE
                app_log.info('Parsing Datetime and Timestamp datatype...')
                parse_dates, timestamp, binary = get_parse_dates(dbconnect=conn, table=table_name)
                if push_query != '*':
                    parse_dates = list(
                        set([item.strip() for item in push_query.split(',')]).intersection(set(parse_dates)))
                    timestamp = list(
                        set([item.strip() for item in push_query.split(',')]).intersection(set(timestamp))),
                    binary = list(set([item.strip() for item in push_query.split(',')]).intersection(set(binary)))
                app_log.info(f'Reading Parent table: {table_name}')
                parent_table_df = pd.read_sql('SELECT {} FROM {}'.format(push_query, table_name),
                                              con=conn,
                                              parse_dates=parse_dates).replace(REPLACER)
                # CONVERTING TIMESTAMP FORMAT INTO HEX STRING
                app_log.info('Creating Compatibility Timestamp Datatype...')
                column = 0
                timestamp_binary = []
                timestamp_binary.extend(timestamp)
                timestamp_binary.extend(binary)
                timestamp_binary = [element for innerList in timestamp_binary for element in innerList]
                while column < len(timestamp_binary):
                    parent_table_df[timestamp_binary[column]] = parent_table_df[timestamp_binary[column]].apply(
                        lambda v: v.hex())
                    parent_table_df[timestamp_binary[column]] = parent_table_df[timestamp_binary[column]].astype(str)
                    column += 1
        # ----------------------------------------------------
        # GETTING TABLE DATA INTO DATAFRAME
        # ----------------------------------------------------

        # ----------------------------------------------------
        # DATAFRAME TO DICTIONARY FOR ELASTICSEARCH DATA
        # ----------------------------------------------------
        # write logic for forming elasticdata from dataframe
        # ----------------------------------------------------
        # DATAFRAME TO DICTIONARY FOR ELASTICSEARCH DATA
        # ----------------------------------------------------

        # ----------------------------------------------------
        # PUSHING TO ELASTICSEARCH
        # ----------------------------------------------------
        # write logic for pushing to elasticsearch
        # ----------------------------------------------------
        # PUSHING TO ELASTICSEARCH
        # ----------------------------------------------------

        except Exception as e:
            app_log.error(e)
            app_log.error(traceback.format_exc())
    end = time.perf_counter()
    app_log.info('Execution time: {}'.format(end - start))


if __name__ == '__main__':
    sync()

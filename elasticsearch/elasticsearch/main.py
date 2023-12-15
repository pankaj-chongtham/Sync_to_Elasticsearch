import pandas as pd
from elasticsearch import Elasticsearch

es=Elasticsearch("http://localhost:9200")
print(es.ping())

#trial comment

def sync():

    # read cmn_elsc_index_tbl to df
    meta_df = pd.read_sql_query('selefct*from ')

    # iterate meta_df
    for index, row in meta_df.iterrows():
        try:
            # get table data
            table_name = 'discrepancy'
            table_df = pd.read_sql_query(f'select*from {table_name}')

            #
        except Exception e:
            print(e)
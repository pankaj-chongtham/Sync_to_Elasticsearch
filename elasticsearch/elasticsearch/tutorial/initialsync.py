import pandas as pd
import pyodbc
from elasticsearch import Elasticsearch, helpers
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=localhost\SQLEXPRESS;'
                        'DATABASE=master;'
                        'Trusted_Connection=yes;')
cursor = conn.cursor()

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

def initialsync():
    cmn_df = pd.read_sql("select * from CMN_Elstc_index_tbl", con=conn)
    condition_records = cmn_df.to_dict(orient='records')
    for record in condition_records:
        index_name = record.get('Elstc_indx_name')
        print(index_name)
        get_id = record.get('Elstc_unique_column_name')
        print(get_id)
        query = record.get('Elstc_push_query')
        print(query)

        elstc_df = pd.read_sql(sql = query, con = conn)
        elstc_dict = elstc_df.to_dict(orient='records')

        res = es.indices.create(index = index_name)
        print(res)

        actions = [
        {
            "_op_type": "index",
            "_index": index_name,
            "_id": doc.get(get_id),
            "_source": doc
        }for doc in elstc_dict
        ]

        success, failed = helpers.bulk(es, actions)
        print(f"Successfully inserted {success} documents.")
        print(f"Failed to insert {failed} documents.")

initialsync()
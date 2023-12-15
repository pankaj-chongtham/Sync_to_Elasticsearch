import pandas as pd
import pyodbc
from elasticsearch import Elasticsearch, helpers
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=DESKTOP-EKJH7K7;'
                        'DATABASE=checkcdc;'
                        'Trusted_Connection=yes;')
cursor = conn.cursor()

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

discrepancy_df = pd.read_sql("select * from discrepancy", con = conn)
#discrepancy_df['discrepencycreationdate'] = discrepancy_df['discrepencycreationdate'].astype(str)
discrepancy_dict = discrepancy_df.to_dict(orient='records')
print(discrepancy_dict)

mapping = {
    "mappings": {
        "properties": {
            "execworkcenter": {
                "type": "text"
            },
            "atano": {
                "type": "long"
            },
            "corrosionrelated": {
                "type": "text"
            }, #trackid not been mapped
            "applicablity": {
                "type": "keyword"
            },
           "description": {
                "type": "text"

            },
            "description_emb": {
                "type": "dense_vector"
            },
            "discrepancytype": {
                "type": "keyword"
            },
	        "execdocno": {
                "type": "text"
            },
            "discrepencycreationdate": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            },
            "aircraftregno": {
                "type": "long"
            }, #currentdiscrepancystatusdesc, zone,	positioncode, corrosionrelateddesc,	sourcetask_discrepancy, customername, partno, doctypedesc, sourcetaskdescription, executionstatus, componentid not mapped
            "execstation": {
                "type": "keyword"
            },
            "ou": {
                "type": "long"
            },
            "majoritem": {
                "type": "keyword"
            }, #execphase, packageno not mapped
	        "partsrequired": {
                "type": "keyword"
            }, #sourcetask not mapped
	        "currentdiscrepancystatus": {
                "type": "keyword"
            },
            "doctype": {
                "type": "keyword"
            },  #execcategory not mapped
	        "discrepancycategory": {
                "type": "text"
            },
            "aircraftmodel": {
                "type": "keyword"
            }, #discrepancyno, taskno, discrepancytypedesc, customercode, execdoctype not mapped
        }
    }
}

#temp = es.index(index='discrepancy', body=mapping)
#print(temp)

actions = [
  {
    "_op_type": "index",
    "_index": "discrepancy",
    "_source": doc
  }for doc in discrepancy_dict
]

success, failed = helpers.bulk(es, actions)
print(f"Successfully inserted {success} documents.")
print(f"Failed to insert {failed} documents.")





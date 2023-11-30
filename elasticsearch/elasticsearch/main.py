from elasticsearch import Elasticsearch

es=Elasticsearch("http://localhost:9200")
print(es.ping())

def create_index():
    return "This will create the index in Elasticsearch"
import elasticsearch
from pprint import pprint

if __name__ == "__main__":
	es = elasticsearch.Elasticsearch()

	es = elasticsearch.Elasticsearch(["cluster-7-slave-00.sl.hackreduce.net:9200"], sniff_on_start=False)
	
	results = es.search(index="twitter", body = 
	{ 
		"size": "7102208", 
		"query": 
			{"match": 
				{"status.text": "obama"} 
			},
		"range":
			{"_score":
				{
				"from": 4
				}
			}
	}
	)

	#pprint(results)

	print "max: ", results['hits']['max_score']

	for result in results['hits']['hits']:
		print result['_score']

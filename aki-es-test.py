import elasticsearch
from pprint import pprint

if __name__ == "__main__":
	es = elasticsearch.Elasticsearch()

	es = elasticsearch.Elasticsearch(["cluster-7-slave-00.sl.hackreduce.net:9200"], sniff_on_start=False)
	
	results = es.search(index="twitter", body = 
	{ 
		"size": "7364415", 
		"query": 
			{"match": 
				{"status.text": "obama"} 
			}
	}
	)

	#pprint(results)

	print "max: ", results['hits']['max_score']
	reslist = []
	negsentimentlist = {}
	possentimentlist = {}

	for result in results['hits']['hits']:
		reslist.append(result['_source']['text'])
		#res = result['_score']
		#if (res > 4):
		#	print res

	f = open('negative-words.txt', 'r')
	for line in f.readlines():
		if (";" in line):
			continue

		line = line.rstrip().lstrip()

		try: 
			for res in reslist:
				if (line in res and line != ''):
					if (line in negsentimentlist):
						negsentimentlist[line] += 1
					else:
						negsentimentlist[line] = 1
		except KeyError:
			print "error: ", line, " | ", res

		except UnicodeDecodeError:
			continue

        f = open('positive-words.txt', 'r')
        for line in f.readlines():
                if (";" in line):
                        continue

                line = line.rstrip().lstrip()

                try:
                        for res in reslist:
                                if (line in res and line != ''):
                                        if (line in possentimentlist):
                                                possentimentlist[line] += 1
                                        else:
                                                possentimentlist[line] = 1
                except KeyError:
                        print "error: ", line, " | ", res

                except UnicodeDecodeError:
                        continue


	pprint(negsentimentlist)
	pprint(possentimentlist)

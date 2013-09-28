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

	#pprint(results['hits']['hits'][0])

	print "max: ", results['hits']['max_score']
	rescounter = 0
	userlist = {}
	tweetlist = {}
	usersentimentlist = {}
	negsentimentlist = {}
	possentimentlist = {}
	totalsentiment = {}

	for result in results['hits']['hits']:
		userlist[rescounter] = result['_source']['user']['screen_name']
		tweetlist[rescounter] = result['_source']['text']
		rescounter += 1
		#res = result['_score']
		#if (res > 4):
		#	print res

	f = open('negative-words.txt', 'r')
	for line in f.readlines():
		if (";" in line):
			continue

		line = line.rstrip().lstrip()

		try: 
			for tweetid, tweet in tweetlist.iteritems():
				if (line in tweet and line != ''):
					if (line in negsentimentlist):
						negsentimentlist[userlist[tweetid]] -= 1
					else:
						negsentimentlist[userlist[tweetid]] = -1
		except KeyError:
			print "error: ", line, " | ", tweet

		except UnicodeDecodeError:
			continue

        f = open('positive-words.txt', 'r')
        for line in f.readlines():
                if (";" in line):
                        continue

                line = line.rstrip().lstrip()

                try:
                        for tweetid, tweet in tweetlist.iteritems():
                                if (line in tweet and line != ''):
                                        if (line in possentimentlist):
                                                possentimentlist[userlist[tweetid]] += 1
                                        else:
                                                possentimentlist[userlist[tweetid]] = 1
                except KeyError:
                        print "error: ", line, " | ", tweet

                except UnicodeDecodeError:
                        continue

	#pprint(negsentimentlist)
	#pprint(userlist)

	lambdaresults = filter(lambda y: y > 3,possentimentlist)
	pprint(lambdaresults)

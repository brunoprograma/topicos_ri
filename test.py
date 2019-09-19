import time
from redisearch import Client, TextField, NumericField, Query
from redis.exceptions import ResponseError

file = open('test_set_tweets.txt', 'r')
client = Client('Tweets')
client.redis.flushdb()
client.create_index([TextField('tweet'), TextField('timestamp')])	
start = time.time()
for x, line in enumerate(file.readlines()):
	content = line.strip().split('\t')
	try:
		if len(content) == 4:  # tem data
			client.add_document('-'.join(content[:2]), tweet=content[-2], timestamp=content[-1])
		else:
			client.add_document('-'.join(content[:2]), tweet=content[-1], timestamp='')
	except ResponseError:
		pass
	if x % 1000 == 0:
	    print(x, 'lines indexed...')        

end = time.time()
print("Indexing time elapsed", end - start)

total = 0
for i in range(30):
	start = time.time()
	res = client.search(Query("@tweet:(ok | fine)"))
	end = time.time()
	total += end-start

print("Query time elapsed", total/30)

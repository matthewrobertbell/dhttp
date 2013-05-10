import os
import time
import string

import redis
from flask import request, Flask, jsonify

app = Flask(__name__)
r = redis.Redis()

hex_digits = set(string.hexdigits.lower())
validate_hash = lambda hash: len(hash) % 16 == 0 and len(hash) <= 128 and all(chr in hex_digits for chr in hash)

byte_seconds = 86400 * 365

@app.route('/', methods=['GET', 'POST'])
def index():
	minute = int(time.time()) / 60
	ip_address = ''
	with r.pipeline() as pipe:
		pipe.incr('ratelimit.'+ip_address)
		pipe.expire('ratelimit.'+ip_address, 60)
		hits, _ = pipe.execute()
	if hits > 180:
		return 'Rate limit for this minute reached, slow down cowboy'
	if request.method == 'POST':
		insert_time = int(time.time())
		with r.pipeline() as pipe:
			for hash, values in request.form.lists():
				if validate_hash(hash):
					longest_expire_time = 0
					for i, v in enumerate(values):
						hash_key = '{hash}-{insert_time}-{i}'.format(hash=hash, insert_time=insert_time, i=i)
						expire_time = byte_seconds / len(v)
						if expire_time > longest_expire_time:
							longest_expire_time = expire_time
						pipe.set(hash_key, v)
						pipe.expire(hash_key, expire_time)
						pipe.sadd('list.'+hash, hash_key)
					pipe.expire('list.'+hash, longest_expire_time)
			pipe.execute()
		return 'ok'
	else:
		hashes = [hash for hash in request.args.getlist('hash') if validate_hash(hash)]
		hash_list_keys = ['list.'+hash for hash in hashes]
		with r.pipeline() as pipe:
			for hash_list_key in hash_list_keys:
				pipe.smembers(hash_list_key)
			hash_lists = pipe.execute()
		with r.pipeline() as pipe:
			for hash_list in hash_lists:
				pipe.mget(hash_list)
			hash_list_values = pipe.execute()

		data = {}
		for keys, values in zip(hash_lists, hash_list_values):		
			for k, v in zip(keys, values):
				insert_time = int(k.split('-')[1])
				hash = k.split('-')[0]
				if hash not in data:
					data[hash] = {}
				if insert_time not in data[hash]:
					data[hash][insert_time] = []
				data[hash][insert_time].append(v)
		return jsonify(**data)

if __name__ == '__main__':
	port = int(os.environ.get('PORT', 5000))
	app.run(host='0.0.0.0', port=port)
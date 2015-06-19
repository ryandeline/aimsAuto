# Downlaods data from Parse.com as JSON objects
# Parses downloaded data
# Writes out data in CSV format with class names as headers
# Python v 3.4

import csv
import json
import sys

import __init__ as parse

try:
	import settings_local
except ImportError:
	raise ImportError('You must create a settings_local.py file with an ' +
                      'APPLICATION_ID, REST_API_KEY, and a MASTER_KEY ' +
                      'to run tests.')

parse.APPLICATION_ID = settings_local.APPLICATION_ID
parse.REST_API_KEY = settings_local.REST_API_KEY

# connection = httplib.HTTPSConnection('api.parse.com', 443)
# # params = urllib.urlencode({"where":json.dumps({
# #     "tailNumber": {
# #         "$in": [
# #             "N1105V"
# #         ]
# #     }
# #     })})
# connection.connect()
# connection.request('GET', '/1/classes/LogEntry?', '', {
#       "X-Parse-Application-Id": "96dGiIHwrajox3Ms0IbTBwzBl4jQe52US5P1KPyb",
#        "X-Parse-REST-API-Key": "fQzHJF4gR8rm6Kto1t6LpuP3OSYg5rGqt27d2YjW"
#     })
#aims = json.loads(connection.getresponse().decode(encoding))
# results = json.loads(connection.getresponse().read())
# print(results)

for parseObject in settings_local.PARSE_CLASSES:
	query = parse.ObjectQuery(parseObject)
	limit = 100
	skip = 0
	counter = 0

	query = query.limit(limit).skip(skip)
	results = query.fetch()

	columns = set()
	for item in results:
		columns.update(set(item.__dict__.keys()))

	with open(parseObject+'.csv', 'w', newline='') as fo:
		#write header
		writer = csv.writer(fo)

	# 	writer.writerow(list(columns))
	# 	for item in results:
	# 		row = []
	# 		for c in columns:
	# 			if c in item: row.append(str(item[c]))
	# 			else: row.append('')
	# 		writer.writerow(row)

		writer.writerow(sorted(list(columns)))
		#write body using limit & skip to get all records in parse
		while 1:
			for item in results:
				counter += 1
				row = []
				for c in sorted(columns):
					if c in item.__dict__:
						if(type(item.__dict__[c]) == parse.Object):
							row.append(str(item.__dict__[c].objectId()))
						else:
							row.append(str(item.__dict__[c]))
					else: row.append('')
				writer.writerow(row)

			if counter == 0:
				break
			else:
				counter = 0
				skip = skip + limit
				query = query.limit(limit).skip(skip)
				results = query._fetch()


# with open('LogEntry.json') as fi:
#     data = json.load(fi)

# json_array = data['results']

# columns = set()
# for item in json_array:
#     columns.update(set(item))


# with open('LogEntry.csv', 'w', newline='') as fo:
#     writer = csv.writer(fo)

#     writer.writerow(list(columns))
#     for item in json_array:
#         row = []
#         for c in columns:
#             if c in item: row.append(str(item[c]))
#             else: row.append('')
#         writer.writerow(row)
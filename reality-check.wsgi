#!/usr/bin/python
#Erik Halperin, 07/01/2014

from cgi import parse_qs
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

def query_wall_time(key):
    # get unique values for key

    unique_vals = []
    for val in coll.distinct(key):
        unique_vals.append(val.encode('ascii', 'ignore'))

    curr_time = 1405382400 # 00:00 GMT, 7/15/14

    days = []
    # get data for each day
    for x in range(0, 7):
        hi = str(curr_time - x * 3600 * 24)
        lo = str(curr_time - (x+1) * 3600 * 24)

        keys = []
        # get wall times for each user/project/site 
        for val in unique_vals:
            crit = { 'CompletionDate': { '$gte': lo, '$lte': hi }, key: val }
            proj = { 'RemoteWallClockTime': 1, '_id': 0 }

            times = coll.find(crit, proj)
            
            # sum wall times
            sum = 0
            for time in times:
                sum += float(time['RemoteWallClockTime'])

            sum = sum / 60 / 60 # seconds to hours

            if sum > 0:
                keys.append({ val: sum })

        days.append({ x: keys })
            
    return days

def application(environ, start_response):
    d = parse_qs(environ['QUERY_STRING'])
    key = d.get('key', [''])[0]
    vals = query_wall_time(key)

    response_body = vals

    response_headers = [('Content-type', 'application/json')]
    status = '200 OK'

    start_response(status, response_headers)

    return json.dumps(response_body, indent=1)

# tries to display error instead of generic 500 Internal Server Error
# courtesy of dgc
def error_capture(app):
    import cgitb

    def wrapper(environ, start_response):
        environ['.contenttype'] = None
        def wrapped_start_response(status, response_headers):
            for name, value in response_headers:
                if name.lower() == 'content-type':
                    environ['.contenttype'] = value
            start_response(status, response_headers)

        try:
            return app(environ, wrapped_start_response)

        except Exception:
            if environ['.contenttype'] is None:
                start_response('500 Error', [('Content-type', 'text/plain')])
                trace = cgitb.text(sys.exc_info())
            elif environ['.contenttype'].lower() == 'text/html':
                trace = cgitb.html(sys.exc_info())
            else:
                trace = cgitb.text(sys.exc_info())

            return [trace]
    return wrapper

application = error_capture(application)

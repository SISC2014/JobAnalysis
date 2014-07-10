#!/usr/bin/python
#Erik Halperin, 07/01/2014

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

    curr_time = 1405054800

    days = []
    # get data for each day
    for x in range(0, 8):
        hi = str(curr_time - x * 3600 * 24)
        lo = str(curr_time - (x+1) * 3600 * 24)

        keys = []
        # get wall times for each user/project/site
        for val in unique_vals:
            crit = { 'JobStartDate': { '$gt': lo, '$lt': hi }, key: val }
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

    #user_vals = query_wall_time('User')
    project_vals = get_unique_vals('ProjectName')

    response_body = project_vals

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

#!/usr/bin/python
# Erik Halperin, 07/17/2014

from cgi import parse_qs # for parsing query strings
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # return json doc
import time # for timestamp

import pwd # mapping username to real name from OSG database
# ***Accessing OSG database (for usernames) requires the program to run on OSG

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

def query_jobs():
    user_list = []

    entries = coll.aggregate( { '$group': { '_id': '$User', 'walltime': { '$sum': '$RemoteWallClockTime' }, 'cputime': { '$sum': '$RemoteUserCpu' }, 'jobs': { '$sum': 1 } } } )

    for entry in entries["result"]:
        # convert username to user's name
        username = entry["_id"].split('@', 1)[0]
        username = pwd.getpwnam(username)
        
        if username.pw_gecos != "":
            entry["_id"] = username.pw_gecos
            
        wt = entry['walltime'] / 60 / 60
        ct = entry['cputime'] / 60 / 60

        entry['walltime'] = "{0:.2f}".format(wt)
        entry['cputime'] = "{0:.2f}".format(ct)

        # change _id to user
        entry['user'] = entry.pop('_id')

        eff = 100 * ct / wt
        entry.update( { 'efficiency': "{0:.2f}".format(eff) } )

        user_list.append(entry)

    return user_list

def application(environ, start_response):
    d = parse_qs(environ['QUERY_STRING'])
    callback = d.get('callback', [''])[0]

    response_body = json.dumps(query_jobs(), indent=1)
    status = '200 OK'

    try:
        response_headers = [('Content-type', 'application/javascript')]
        response_body = callback + '(' + response_body + ');'
        start_response(status, response_headers)
        return response_body
    except Exception:
        pass

    response_headers = [('Content-type', 'application/json')]
    start_response(status, response_headers)

    return response_body

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

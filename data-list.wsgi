#!/usr/bin/python
# Erik Halperin, 07/01/2014

from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time

import pwd # mapping username to real name from OSG database
# ***Accessing OSG database (for usernames) requires the program to run on OSG***

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

def query_data():

    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    # secs_ago = time.time() - hours * 60 * 60
    # criteria = { 'JobStartDate': { '$gt': secs_ago } }

    sites = coll.distinct(resource)
    users = coll.distinct('User')
    projects = coll.distinct('ProjectName')

    # convert username to user's name
    for index, user in enumerate(users):
        try:
            name = user.split('@', 1)[0]
            name = pwd.getpwnam(name)
            users[index] = name.pw_gecos
        except Exception:
            pass


    return { 'users': users, 'projects': projects, 'sites': sites }

def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    # hours = int(d.get('hours', [''])[0])
    callback = d.get('callback', [''])[0]

    status = '200 OK'
    response_body = []
    response_body = json.dumps(query_data(), indent=2)
    response_headers = [('Content-type', 'application/javascript')]
    response_body = callback + '(' + response_body + ');'
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

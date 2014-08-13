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

def query_jobs(hours):
    resource = '$MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = time.time() - hours * 60 * 60

    match = { '$match': { 'CompletionDate': { '$gte': secs_ago } } }
    group = { '$group': { '_id': { 'user': '$User', 'project': '$ProjectName'}, 'walltime': { '$sum': '$RemoteWallClockTime' }, 'cputime': { '$sum': '$RemoteUserCpu' }, 'jobs': { '$sum': 1 } } }

    entries = coll.aggregate([match, group])

    ret_list = []

    for entry in entries['result']:
        # sometimes jobs don't have a project, so entries without a project should be skipped (for now)
        try:
            # change username to user's name
            user = entry['_id']['user']
            name = user.split('@', 1)[0]
            name = pwd.getpwnam(name)
            name = name.pw_gecos
            
            # if user doesn't have name, use their username
            if name != "":
                user = name

            # calculate efficiency
            wt = entry['walltime'] / 60 / 60
            ct = entry['cputime'] / 60 / 60
            eff = 100 * ct / wt

            # format numbers to 1 decimal place
            eff = "{0:.1f}".format(eff)
            wt = "{0:.1f}".format(wt)
            ct = "{0:.1f}".format(ct)
            
            d = { 'user': user, 'project': entry['_id']['project'], 'jobs': entry['jobs'], 'walltime': wt, 'cputime': ct, 'efficiency': eff }
            ret_list.append(d)
        except Exception:
            pass

    return { 'data': ret_list }


def application(environ, start_response):
    # parse url parameters
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])

    status = '200 OK'

    response_body = json.dumps(query_jobs(hours), indent=2)

    # return jsonp with callback
    response_headers = [('Content-type', 'application/javascript')]
    #response_body = callback + '(' + response_body + ');'
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

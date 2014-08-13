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

def query_jobs(hours, users):
    resource = '$MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = time.time() - hours * 60 * 60

    # query all users if users is None or else just the specified user(s)
    if users is not None:
        for index, user in enumerate(users):
            user = user + '@login01.osgconnect.net'
            users[index] = { 'User': user }

        match = { '$match': { 'CompletionDate': { '$gte': secs_ago }, '$or': users } }
    else:
        # query all users
        match = { '$match': { 'CompletionDate': { '$gte': secs_ago } } }

    # get total wall time, cpu time, & jobs for each user, project, and site
    group = { '$group': { '_id': { 'user': '$User', 'project': '$ProjectName', 'site': resource }, 'walltime': { '$sum': '$RemoteWallClockTime' }, 'cputime':{ '$sum': '$RemoteUserCpu' }, 'jobs': { '$sum': 1 } } }

    entries = coll.aggregate([match, group])

    # Entries is a series of dictionaries where each one is a uniqe combination of a project, user, and site
    # We want each user to have a list of projects that each have a list of sites, so we must do some modifications

    total_list = []

    # for debugging
    missed_entries = 0

    for entry in entires['result']:
        project = entry.get('_id').get('project')
        user = entry.get('_id').get('user')
        site = entry.get('_id').get('site')
        user_dict = {}

        # sometimes jobs don't have a project or site, so entries without a project/site should be discarded
        if project is None or site is None:
            missed_entries += 1
            continue

        # check to see if this user has already been added to user_dict
        if user not in user_dict.values():
            user_dict.update( { 'user': user, 'projects': [] } )

        # check to see if this project has already been added to it's user in user_dict
        if project not in user_cache.get(

def application(environ, start_response):
    # parse url parameters
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])
    users = d.get('users', [''])[0]
    callback = d.get('callback', [''])[0]

    if users != "":
        response_body = query_jobs(hours, users.split(','))
        response_body = json.dumps(response_body, indent=2)
    else:
        response_body = query_jobs(hours, None)
        response_body = json.dumps(response_body, indent=2)

    status = '200 OK'

    # return jsonp with callback
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

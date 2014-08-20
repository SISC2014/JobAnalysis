#!/usr/bin/python
# Erik Halperin, 07/01/2014

from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time # timestamps
import redis # ca-ching $$$

import pwd # mapping username to real name from OSG database
# ***Accessing OSG database (for usernames) requires the program to run on OSG***

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

def query_data():
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'

    # should probs add parallel execution
    sites = coll.distinct(resource)
    users = coll.distinct('User')
    projects = coll.distinct('ProjectName')

    # convert username to user's name
    '''for index, user in enumerate(users):
        try:
            name = user.split('@', 1)[0]
            name = pwd.getpwnam(name)
            users[index] = name.pw_gecos
        except Exception:
            pass
    '''

    return { 'users': users, 'projects': projects, 'sites': sites }

# TODO - add hours
def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    callback = d.get('callback', [''])[0]

    timestamp = time.time()

    # initialize redis
    r_server = redis.Redis('db.mwt2.org')

    users = r_server.lrange('users2', 0, -1)
    projs = r_server.lrange('projects', 0, -1)
    sites = r_server.lrange('sites', 0, -1)

    # return data from redis
    if(r_server.get('ts') >= timestamp - 60*30):
        response_body = json.dumps( { 'users': users, 'projects': projs, 'sites': sites }, indent=2)

    # query mongo
    else:        
        r_server.set('ts', timestamp)
        data = query_data()
        
        # get users, projs, and sites not already in cache
        new_users = [item for item in data['users'] if item not in users]
        new_projs = [item for item in data['projects'] if item not in projs]
        new_sites = [item for item in data['sites'] if item not in sites]
        
        # update redis
        for user in new_users:
            r_server.rpush('users2', user)

        for proj in new_projs:
            r_server.rpush('projects', proj)

        for site in new_sites:
            r_server.rpush('sites', site)
        
        users = r_server.lrange('users2', 0, -1)
        projs = r_server.lrange('projects', 0, -1)
        sites = r_server.lrange('sites', 0, -1)

        response_body = json.dumps( { 'users': users, 'projects': projs, 'sites': sites }, indent=2)

    status = '200 OK'

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

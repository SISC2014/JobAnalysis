#!/usr/bin/python
# Erik Halperin, 07/01/2014

from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time # timestamps
import redis # ca-ching $$$
import math # for bin calculations

import pwd # mapping username to real name from OSG database
# ***Accessing OSG database (for usernames) requires the program to run on OSG***

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records
r_server = redis.Redis('db.mwt2.org')

def get_index(group, item):
    try:
        index = r_server.lrange(group, 0, -1).index(item)
    except Exception:
        r_server.rpush(group, item)
        index = r_server.lrange(group, 0, -1).index(item)

    return index

def job_count(hours, user, proj, site, bin):
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    resource2 = '$' + resource
    secs_ago = time.time() - hours * 60 * 60

    entry_list = []

    match = { '$match': { 'CompletionDate': { '$gte': secs_ago }, 'User': user } }
    group = { '$group': { '_id': { 'time': '$CompletionDate', 'user': '$User', 'project': '$ProjectName', 'site': resource2 }, 'jobs': { '$sum': 1 } } }

    entries = coll.aggregate([match, group])

    # sort entries into bin groups
    bin_s = bin * 60
    for entry in entries['result']:
        try:
            project = entry['_id']['project']
            user = entry['_id']['user']
            site = entry['_id']['site']
            secs = entry['_id']['time']
            jobs = entry['jobs']

            # sort entries into bin groups
            diff = secs - secs_ago
            bin_num = math.floor((diff + 0.0) / bin_s)
            bin_ts = bin_num * bin_s + secs_ago

            # convert user, project, and site into numbers
            entry = [user, project, site, bin_ts, entry['jobs']]

            entry_list.append(entry)
        except Exception:
            continue

    # combine entries with same user, project, site, and timestamp
    merged = {}
    for item in entry_list:
        key = item[0] + ':' + item[1] + ':' + item[2] + ':' + str(item[3])
        if key not in merged:
            merged.update({ key: item[4] })
        else:
            merged[key] += item[4]

    # format merged properly
    ret_list = []
    for key in merged.keys():
        entry = []
        vals = key.split(':')
        entry = [get_index('users2', vals[0]), get_index('projects', vals[1]), get_index('sites', vals[2]), float(vals[3]), merged[key]]
        ret_list.append(entry)

    return ret_list

def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    callback = d.get('callback', [''])[0]
    user = d.get('user', [''])[0]
    project = d.get('project', [''])[0]
    site = d.get('site', [''])[0]
    bin = d.get('bin', [''])[0]
    hours = d.get('hours', [''])[0]

    curr_time = time.time()
    entry_list = []

    status = '200 OK'

    response_body = json.dumps(job_count(hours, user, project, 'MWT2', bin), indent=2)

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

'''
def redis_search(user, project):
    user_index = r_server.smembers('users').index(user)
    proj_index = r_server.smembers('projects').index(project)

    entry_list = []

    for site_index, site in enumerate(r_server.smembers('sites')):
        entry = []
        entry.append(user_index) # user index
        entry.append(proj_index) # project index
        entry.append(site_index) # site index

        data = r_server.get(user + ':' + project + ':' + site)
        # entry.append(data[1]) # timestamp
        entry.append(data[0]) # job count

        entry_list.append(entry)

    return entry_list
'''

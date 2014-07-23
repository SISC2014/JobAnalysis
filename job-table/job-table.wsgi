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
    job_list = []
    resource = '$MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = time.time() - hours * 60 * 60

    # get only jobs that completed before after secs_ago
    match = { '$match': { 'CompletionDate': { '$gte': secs_ago } } }
    # get total wall time, cpu time, & jobs for each user, project, and site
    group = { '$group': { '_id': { 'user': '$User', 'project': '$ProjectName', 'site': resource }, 'walltime': { '$sum': '$RemoteWallClockTime' }, 'cputime': { '$sum': '$RemoteUserCpu' }, 'jobs': { '$sum': 1 } } }

    entries = coll.aggregate([match, group])

    # return entries

    user_list = []
    project_list = []
    site_list = []
    user_cache = {}

    # for debugging
    the_list = []
    missed_jobs = 0
    entry_count = 0

    for entry in entries["result"]:
        entry_count += 1
        project = entry.get('_id').get('project')
        user = entry.get('_id').get('user')
        site = entry.get('_id').get('site')

        name = user.split('@', 1)[0]
        name = pwd.getpwnam(name)
        name = name.pw_gecos

        if name != "":
            user = name

        # sometimes jobs don't have a project or site, so entries without a job/site should be discarded
        if project is None or site is None:
            missed_jobs += 1
            continue

        if user not in user_cache:
            user_list.append(user)
            user_cache.update( { user: {} } )

        if project not in user_cache.get(user):
            project_list.append(project)
            user_cache.get(user).update( { project: {} } ) #lower

        # should always enter this if statment
        if site not in user_cache.get(user).get(project): #lower
            site_list.append(site)
            # compute wall & cpu time in hours
            wt = entry['walltime'] / 60 / 60
            ct = entry['cputime'] / 60 / 60

            # calculate efficiency: cpu time / wall time
            eff = 100 * ct / wt
            eff = "{0:.2f}".format(eff)

            # format to two decimal places
            wt = "{0:.2f}".format(wt)
            ct = "{0:.2f}".format(ct)

            user_cache.get(user).get(project).update( { site: { 'efficiency': eff, 'walltime': wt, 'cputime': ct, 'jobs': entry['jobs'] } } )

    the_list.append(user_cache)
    the_list.append({'missed': missed_jobs})
    the_list.append({'entries': entry_count})
    return the_list

    '''for entry in entries["result"]:
        # convert username to user's name
        username = entry["_id"].split('@', 1)[0]
        username = pwd.getpwnam(username)

        if username.pw_gecos != "":
            entry["_id"] = username.pw_gecos

        # compute wall & cpu time in hours
        wt = entry['walltime'] / 60 / 60
        ct = entry['cputime'] / 60 / 60

        # format to two decimal places
        entry['walltime'] = "{0:.2f}".format(wt)
        entry['cputime'] = "{0:.2f}".format(ct)

        # change _id to user
        entry['user'] = entry.pop('_id')

        # calculate efficiency: cpu time / wall time
        eff = 100 * ct / wt
        entry.update( { 'efficiency': "{0:.2f}".format(eff) } )

        job_list.append(entry)

    return job_list'''

def application(environ, start_response):
    # parse url parameters
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])
    callback = d.get('callback', [''])[0]

    response_body = json.dumps(query_jobs(hours), indent=2)
    status = '200 OK'

    # first try returning jsonp if callback function is included
    try:
        response_headers = [('Content-type', 'application/javascript')]
        response_body = callback + '(' + response_body + ');'
        start_response(status, response_headers)
        return response_body
    except Exception:
        pass

    # otherwise return json
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

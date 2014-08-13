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

def modify_dict(d):
    d_list = []

    # iterate over each key (user)
    for user_key in d.keys():
        d_user = { 'user': user_key, 'projects': [] }
        
        # iterate over each project key
        for project_key in d[user_key].keys():
            d_project = { 'project': project_key, 'sites': [] }
            
            # iterate over each site
            for site_key in d[user_key][project_key].keys():
                site = d[user_key][project_key][site_key]

                d_site = {'site':site_key, 'efficiency':site['efficiency'], 'jobs':site['jobs'], 'cputime':site['cputime'], 'walltime':site['walltime']}
                d_project['sites'].append(d_site)
                
            d_user['projects'].append(d_project)

        d_list.append(d_user)

    return { 'data': d_list }

def query_jobs(hours, users):
    resource = '$MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = time.time() - hours * 60 * 60

    if users is not None:
        for index, user in enumerate(users):
            user = user + '@login01.osgconnect.net'
            users[index] = { 'User': user }

        # get only jobs that completed before after secs_ago with specified users
        match = { '$match': { 'CompletionDate': { '$gte': secs_ago }, '$or': users } }
    else:
        # get jobs for all users
        match = { '$match': { 'CompletionDate': { '$gte': secs_ago } } }

    # get total wall time, cpu time, & jobs for each user, project, and site
    group = { '$group': { '_id': { 'user': '$User', 'project': '$ProjectName', 'site': resource }, 'walltime': { '$sum': '$RemoteWallClockTime' }, 'cputime': { '$sum': '$RemoteUserCpu' }, 'jobs': { '$sum': 1 } } }

    entries = coll.aggregate([match, group])

    # Entries is a series of dictionaries where each one is a uniqe combination of a project, user, and site
    # We want each user to have a list of projects that each have a list of sites

    total_list = []
    user_cache = {}

    # for debugging
    missed_jobs = 0
    entry_count = 0

    for entry in entries["result"]:
        entry_count += 1
        project = entry.get('_id').get('project')
        user = entry.get('_id').get('user')
        site = entry.get('_id').get('site')

        # change username to user's name
        name = user.split('@', 1)[0]
        name = pwd.getpwnam(name)
        name = name.pw_gecos

        if name != "":
            user = name

        # sometimes jobs don't have a project or site, so entries without a project/site should be discarded
        if project is None or site is None:
            missed_jobs += 1
            continue

        if user not in user_cache:
            user_cache.update( { user: {} } )

        if project not in user_cache.get(user):
            user_cache.get(user).update( { project: {} } )

        # should always enter this if statment
        if site not in user_cache.get(user).get(project):
            # compute wall & cpu time in hours
            wt = entry['walltime'] / 60 / 60
            ct = entry['cputime'] / 60 / 60

            # calculate efficiency: cpu time / wall time
            eff = 100 * ct / wt
            eff = "{0:.1f}".format(eff)

            # format to two decimal places
            wt = "{0:.1f}".format(wt)
            ct = "{0:.1f}".format(ct)

            user_cache.get(user).get(project).update( { site: { 'efficiency': eff, 'walltime': wt, 'cputime': ct, 'jobs': entry['jobs'] } } )

    # Create totals for each user and each project
    user_list = user_cache.keys()
    for user in user_list:
        user_eff_total, user_wall_total, user_cpu_total, user_jobs_total = 0, 0, 0, 0
        project_list = user_cache.get(user).keys()

        for project in project_list:
           project_eff_total, project_wall_total, project_cpu_total, project_jobs_total = 0, 0, 0, 0
           site_list = user_cache.get(user).get(project).keys()

           for site in site_list:
               access = user_cache.get(user).get(project).get(site)

               project_eff_total += float(access.get('efficiency')) / len(site_list)
               project_wall_total += float(access.get('walltime'))
               project_cpu_total += float(access.get('cputime'))
               project_jobs_total += float(access.get('jobs'))

           # Get each project total
           project_data = { 'Total': { 'efficiency': "{0:.1f}".format(project_eff_total), 'walltime': "{0:.1f}".format(project_wall_total), 'cputime': "{0:.1f}".format(project_cpu_total), 'jobs': project_jobs_total } }

           user_cache.get(user).get(project).update(project_data)

           user_eff_total += project_eff_total / len(project_list)
           user_wall_total += project_wall_total
           user_cpu_total += project_cpu_total
           user_jobs_total += project_jobs_total

        # Only have a user total if there are multiple projects
        if len(project_list) > 1:
            user_cache.get(user).update( { 'User Total': { ' ':  { 'efficiency': "{0:.1f}".format(user_eff_total), 'walltime': "{0:.1f}".format(user_wall_total), 'cputime': "{0:.1f}".format(user_cpu_total), 'jobs': user_jobs_total } } } )

    total_list.append(user_cache)
    total_list.append({'missed': missed_jobs})
    total_list.append({'entries': entry_count})
    return total_list

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

        response_body = modify_dict(response_body[0])

        response_body = json.dumps(response_body, indent=2)

    status = '200 OK'

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

#!/usr/bin/python
# Erik Halperin, 07/01/2014

from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time # for timestamp
import urllib2, sys # converting ip address to geo coordinates

import pwd # mapping username to real name from OSG database
# ***Accessing OSG database requires the program to run on OSG***

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

# ca-ching $$$
site_cache = {}
user_cache = {}
project_cache = {}
resource_cache = {}


def get_cds(host):
    worked = 0

    try:
        req = urllib2.Request("http://geoip.mwt2.org:4288/json/"+host, None)
        opener = urllib2.build_opener()
        f = opener.open(req, timeout=5)
        res = json.load(f)
        lon = res['longitude']
        lat = res['latitude']
        worked = 1

        return [lat, lon]

    except Exception:
        pass
    if not worked:
        try:
           req = urllib2.Request("http://freegeoip.net/json/"+host, None)
           opener = urllib2.build_opener()
           f = opener.open(req, timeout=5)
           res = json.load(f)
           lon = res['longitude']
           lat = res['latitude']

           return [lat, lon]

        except Exception:
            # if ip address can't be resolved to geo coordinates, return impossible ones to be removed later
            return [-200, -200]

def modify(job):
    global user_cache, site_cache, resource_cache, project_cache
    # convert all strings to ints
    job['JobStartDate'] = int(job['JobStartDate'])
    job['CompletionDate'] = int(job['CompletionDate'])
    job['RemoteWallClockTime'] = float(job['RemoteWallClockTime'])
    job['RemoteUserCpu'] = float(job['RemoteUserCpu'])

    # change User to actual User's name
    username = job['User']
    if username in user_cache:
        job['User'] = user_cache.get(username)
    else:
        username = re.sub('[\"]', '', job['User'])
        username = username.split('@', 1)[0]
        username = pwd.getpwnam(username)

        user_cache.update({ job['User']: username.pw_gecos })

        job['User'] = username.pw_gecos

    # remove unncessary chars from ProjectName and site
    project = job['ProjectName']
    if project in project_cache:
        job['ProjectName'] = project_cache.get(project)
    else:
        project = re.sub('[\"]', '', job['ProjectName'])
        user_cache.update({ job['ProjectName']: project })
        job['ProjectName'] = project

    resource = job['MATCH_EXP_JOBGLIDEIN_ResourceName']
    if resource in resource_cache:
        job['MATCH_EXP_JOBGLIDEIN_ResourceName'] = resource_cache.get(resource)
    else:
        resource = re.sub('[\"]', '', job['MATCH_EXP_JOBGLIDEIN_ResourceName'])
        resource_cache.update({ job['MATCH_EXP_JOBGLIDEIN_ResourceName']: resource })
        job['MATCH_EXP_JOBGLIDEIN_ResourceName'] = resource

    # convert StartdPrincipal to coordinates
    site = job['StartdPrincipal']
    if site in site_cache:
        cds = site_cache.get(site)
    else:
        site = re.sub('[\"]', '', job['StartdPrincipal'])
        site = site.split('/', 1)[-1]
        cds = get_cds(site)
        site_cache.update({ job['StartdPrincipal']: cds })

    job['StartdPrincipal'] = cds[0] # latitude
    job['longitude'] = cds[1]

    # make key names nice + lowercase
    job['latitude'] = job.pop('StartdPrincipal')
    job['starttime'] = job.pop('JobStartDate')
    job['endtime'] = job.pop('CompletionDate')
    job['walltime'] = job.pop('RemoteWallClockTime')
    job['cputime'] = job.pop('RemoteUserCpu')
    job['jobid'] = job.pop('_id')
    job['user'] = job.pop('User')
    job['project'] = job.pop('ProjectName')
    job['clusterid'] = job.pop('ClusterId')
    job['site'] = job.pop('MATCH_EXP_JOBGLIDEIN_ResourceName')

    return job

def query_jobs(hours):
    jobs = []
    secs_ago = str(time.time() - hours * 60 * 60)

    crit = { 'JobStartDate': { '$gt': secs_ago } }
    proj = { 'JobStartDate': 1, 'CompletionDate': 1, 'RemoteWallClockTime': 1, 'RemoteUserCpu': 1, \
             'StartdPrincipal': 1, 'ProjectName': 1, 'User': 1, 'ClusterId': 1, 'MATCH_EXP_JOBGLIDEIN_ResourceName': 1 }

    for condor_history in coll.find(crit, proj):
        if 'StartdPrincipal' in condor_history:
            job = modify(condor_history)
            # if job's lat is -200, don't append it
            if job.get('latitude') != -200:
                jobs.append(job)

    return jobs

def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])
    callback = d.get('callback', [''])[0]

    response_body = json.dumps(query_jobs(hours), indent=1)

    status = '200 OK'

    # returns JSONP if callback function is included
    try:
        response_headers = [('Content-type', 'application/javascript')]
        response_body = callback + '(' + response_body + ');'
        start_response(status, response_headers)
        return response_body
    except Exception:
        pass

    # otherwise return JSON
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

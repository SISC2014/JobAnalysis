#!/usr/bin/python
# Erik Halperin, 07/01/2014

from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time # for timestamp
import urllib2, sys # converting ip address to geo coordinates
import subprocess # mapping username to real name in system

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

# global cache for site to coordinate mapping
site_cache = {}

# cache for username to name mapping
user_cache = {}

def get_cds(host):
    worked = 0
    global site_cache

    # check cache for value
    if host in site_cache:
        return site_cache.get(host)

    try:
        req = urllib2.Request("http://geoip.mwt2.org:4288/json/"+host, None)
        opener = urllib2.build_opener()
        f = opener.open(req, timeout=5)
        res = json.load(f)
        lon = res['longitude']
        lat = res['latitude']
        worked = 1

        # update cache
        site_cache.update({ host: [lat, lon] })

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
           site_cache.update({ host: [lat, lon] })

           return [lat, lon]

        except Exception:
            # if ip address can't be resolved to geo coordinates, return impossible ones to be removed later
            site_cache.update({ host: [-200,-200] })
            return [-200, -200]

def modify(job):
    global user_cache
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

        p1 = subprocess.Popen(['finger', username], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['sed', '-e', '/Name/!d', '-e', 's/.*Name: //'], stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()

        username = re.sub('[\n]', '', p2.communicate()[0])
        user_cache.update({ job['User']: username })

        job['User'] = username

    # remove unncessary chars from ProjectName
    if 'ProjectName' in job:
       job['ProjectName'] = re.sub('[\"]', '', job['ProjectName'])

    # convert StartdPrincipal to coordinates
    site = re.sub('[\"]', '', job['StartdPrincipal'])
    site = site.split('/', 1)[-1]
    cds = get_cds(site)
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

    return job

def query_jobs(hours):
    jobs = []
    secs_ago = str(time.time() - hours * 60 * 60)

    crit = { 'JobStartDate': { '$gt': secs_ago } }
    proj = { 'JobStartDate': 1, 'CompletionDate': 1, 'RemoteWallClockTime': 1, 'RemoteUserCpu': 1, \
             'StartdPrincipal': 1, 'ProjectName': 1, 'User': 1, 'ClusterId': 1 }

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

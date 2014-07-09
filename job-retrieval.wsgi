#!/usr/bin/python
'''
Erik Halperin, 07/01/2014

'''
from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time # for timestamp
import urllib2, sys # converting ip address to geo coordinates

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

# global cache for site to coordinate mapping
site_cache = {}

def get_cds(host):
    worked = 0
    global site_cache

    # check cache for value
    if host in site_cache:
       return site_cache[host]

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
           site_cache.update({ host: [0] })
           return [0] # failure

def modify(job):
    # convert all strings to ints
    job['JobStartDate'] = int(job['JobStartDate'])
    job['CompletionDate'] = int(job['CompletionDate'])
    job['RemoteWallClockTime'] = float(job['RemoteWallClockTime'])
    job['RemoteUserCpu'] = float(job['RemoteUserCpu'])

    # remove unnecessary characters from User
    user = re.sub('[\"]', '', job['User'])
    job['User'] = user.split('@', 1)[0]

    # remove unncessary chars from ProjectName
    if 'ProjectName' in job:
       job['ProjectName'] = re.sub('[\"]', '', job['ProjectName'])

    # convert StartdPrincipal to coordinates
    site = re.sub('[\"]', '', job['StartdPrincipal'])
    site = site.split('/', 1)[-1]
    job['StartdPrincipal'] = get_cds(site)

    # make key names nice
    job['Project'] = job.pop('ProjectName')
    job['Coordinates'] = job.pop('StartdPrincipal')
    job['StartTime'] = job.pop('JobStartDate')
    job['EndTime'] = job.pop('CompletionDate')
    job['WallTime'] = job.pop('RemoteWallClockTime')
    job['CPUTime'] = job.pop('RemoteUserCpu')
    job['JobId'] = job.pop('_id')

    return job

def query_jobs(hours):
    jobs = []
    secs_ago = str(time.time() - hours * 60 * 60)

    crit = { 'JobStartDate': { '$gt': secs_ago } }
    proj = { 'JobStartDate': 1, 'CompletionDate': 1, 'RemoteWallClockTime': 1, 'RemoteUserCpu': 1, \
             'StartdPrincipal': 1, 'ProjectName': 1, 'User': 1, 'ClusterId': 1 }

    for condor_history in coll.find(crit, proj):
        if 'StartdPrincipal' in condor_history:
           jobs.append(modify(condor_history))

    return jobs

def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])
    callback = d.get('callback', [''])[0]

    response_body = json.dumps(query_jobs(hours))

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
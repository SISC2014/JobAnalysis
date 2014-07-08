'''
Erik Halperin, 07/01/2014

'''
from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import unicodedata # convert unicode strings to english
import datetime, time # for converting unix time
import urllib2, sys # converting ip address to geo coordinates

# connect to database
client = MongoClient('mc.mwt2.org', 27017)
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

    # convert LastRemoteHost to coordinates
    if 'LastRemoteHost' in job:
       site = re.sub('[\"]', '', job['LastRemoteHost'])
       site = site.split('@', 1)[-1]
       site = site.split('.', 1)[-1]
       job['LastRemoteHost'] = get_cds(site)

    return job


def query_jobs(hours):
    jobs = []
    secs_ago = str(time.time() - hours * 60 * 60)

    crit = { 'JobStartDate': { '$gt': secs_ago } }
    proj = { 'JobStartDate': 1, 'CompletionDate': 1, 'RemoteWallClockTime': 1, 'RemoteUserCpu': 1, \
             'LastRemoteHost': 1, 'ProjectName': 1, 'User': 1, 'ClusterId': 1 }

    for condor_history in coll.find(crit, proj):
        jobs.append(modify(condor_history))

    # temporary (terrible) fix for geolocation problems
    jobs_new = []
    for job in jobs:
        if job.get('LastRemoteHost') != [51.5, -0.13]:
           if job.get('LastRemoteHost') != [0]:
              jobs_new.append(job)

    return jobs_new


def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])

    response_body = query_jobs(hours)

    response_headers = [('Content-type', 'application/json')]
    status = '200 OK'
    start_response(status, response_headers)

    return json.dumps(response_body)

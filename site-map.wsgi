#!/usr/bin/python
# Erik Halperin, 07/01/2014

# TODO: aggregation once mongodb is changed

from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import time # for timestamp
import urllib2 # converting ip address to geo coordinates

import pwd # mapping username to real name from OSG database --> NOT USED CURRENTLY
# ***Accessing OSG database (for usernames) requires the program to run on OSG***

# connect to database
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

def get_cds(host):
    worked = 0

    try:
        # try mwt2's instance of geoip
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

    # use freegeoip
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

def query_jobs(hours, sites):
    jobs = []
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = str(time.time() - hours * 60 * 60)

    for site in sites:
        site_dict = { 'site': re.sub('[\"]', '', site[0]) } # remove excess quotes

        # get each project for each site
        criteria = { 'JobStartDate': { '$gt': secs_ago }, resource : site[0] }

        project_names = coll.find(criteria).distinct('ProjectName')

        # get sum of wall times & total jobs for each project
        projects = []

        for project in project_names:
                proj_dict = {'project': re.sub('[\"]', '', project) }

                # TODO: Once db updated, use $sum
                criteria2 = { 'JobStartDate': { '$gt': secs_ago }, resource: site[0], 'ProjectName': project }
                projection2 = { 'RemoteWallClockTime': 1 }

                wall_time_tot = 0
                for wall_time in coll.find(criteria2, projection2):
                    if 'RemoteWallClockTime' in wall_time:
                        wall_time_tot += float(wall_time['RemoteWallClockTime'])

                proj_dict.update( { 'walltime': wall_time_tot/60/60 } ) # seconds to hours
                proj_dict.update( { 'jobs': coll.find(criteria2).count() } ) # get number of jobs
                projects.append(proj_dict)

        # get coordinates
        cds = get_cds(site[1])

        # add coords & job info to site_dict
        site_dict.update( {'latitude': cds[0] } )
        site_dict.update( {'longitude': cds[1] } )
        site_dict.update( {'projects': projects } )

        jobs.append(site_dict)

    return jobs

def get_sites(hours):
    sites_and_ips = []
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = str(time.time() - hours * 60 * 60)

    crit = { 'JobStartDate': { '$gt': secs_ago } }
    proj = { resource: 1, '_id': 0, 'StartdPrincipal': 1 }

    for condor_history in coll.find(crit).distinct(resource): # get each unique site
        site = condor_history.encode('ascii', 'ignore') # convert unicode to ascii
        # sites have multiple different ip address, but they are all at the same place so we only need one
        ip_addr = coll.find_one( { resource: site }, { '_id': 0, 'StartdPrincipal': 1 } )
        ip_addr = ip_addr['StartdPrincipal'].encode('ascii', 'ignore').split('/', 1)[-1].split("\"", 1)[0] # remove excess chars

        sites_and_ips.append( [site, ip_addr] )

    return sites_and_ips # return package of [site, ip_address]

def get_one_site(hours, site):
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    crit = { resource: site }
    proj = { '_id': 0, 'StartdPrincipal': 1 }

    ip = coll.find_one(crit, proj).get('StartdPrincipal').encode('ascii', 'ignore').split('/', 1)[-1].split("\"", 1)[0]
    return [site, ip]


def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    hours = int(d.get('hours', [''])[0])
    callback = d.get('callback', [''])[0]
    site = d.get('site', [''])[0]
    one_site = 0

    # try to get data for one specified site
    try:
        site = "\"" + site + "\""
        response_body = json.dumps(query_jobs(hours, [get_one_site(hours, site)]), indent=1)
        one_site = 1
    except Exception:
        pass

    # get data for ALL sites
    if one_site is 0:
        response_body = json.dumps(query_jobs(hours, get_sites(hours)), indent=1)

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

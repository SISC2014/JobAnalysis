#!/usr/bin/python
# Erik Halperin, 07/01/2014

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
    site_list = []
    resource = '$MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = time.time() - hours * 60 * 60

    entries = coll.aggregate([{'$match':{'CompletionDate':{'$gte':secs_ago}}},{'$group':{'_id':{'project':'$ProjectName','site':resource},'wallhours':{'$sum':'$RemoteWallClockTime'},'count':{'$sum':1}}}])["result"]

    for site in sites:
        # get site coordinates
        cds = get_cds(site[1])

        site_dict = { 'site': site[0], 'latitude': cds[0], 'longitude': cds[1] }

        project_list = []

        for entry in entries:
            if entry["_id"].get('site') == site[0]:
                proj_dict = { 'wallhours': entry['wallhours']/60/60, 'jobs': entry['count'], 'project': entry['_id'].get('project') }
            else:
                continue

            project_list.append(proj_dict)

        site_dict.update( { 'projects': project_list } )

        site_list.append(site_dict)

    return site_list

def get_sites(hours):
    sites_and_ips = []
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    secs_ago = time.time() - hours * 60 * 60

    crit = { 'JobStartDate': { '$gt': secs_ago } }

    # get each unique site
    for condor_history in coll.find(crit).distinct(resource):
        # convert unicode to ascii
        site = condor_history.encode('ascii', 'ignore')
        # sites have multiple different ip address, but they are all at the same place so we only need one
        ip_addr = coll.find_one( { resource: site }, { '_id': 0, 'StartdPrincipal': 1 } )
        # remove excess chars from ip addr
        ip_addr = ip_addr['StartdPrincipal'].encode('ascii', 'ignore').split('/', 1)[-1]

        sites_and_ips.append( [site, ip_addr] )

    return sites_and_ips # return package of [site, ip_address]

def get_one_site(hours, site):
    resource = 'MATCH_EXP_JOBGLIDEIN_ResourceName'
    crit = { resource: site }
    proj = { '_id': 0, 'StartdPrincipal': 1 }

    ip = coll.find_one(crit, proj).get('StartdPrincipal').encode('ascii', 'ignore').split('/', 1)[-1]
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

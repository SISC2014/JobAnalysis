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

def get_coord(host):
    worked = 0
    try:
        req = urllib2.Request("http://geoip.mwt2.org:4288/json/"+host, None)
        opener = urllib2.build_opener()
        f = opener.open(req, timeout=5)
        res = json.load(f)
        lon = res['longitude']
        lat = res['latitude']
        worked = 1
        return [lon, lat]
    except:
        return ['f1a']
        if debug:
           # do something
           return ['f1b']
    if not worked:
        try:
           return ['s2']
           req = urllib2.Request("http://freegeoip.net/json/"+host, None)
           opener = urllib2.build_opener()
           f = opener.open(req, timeout=5)
           res = json.load(f)
           lon = res['longitude']
           lat = res['latitude']
           return [lon, lat]
        except:
           return ['f2']


def str_to_num(lst):
    new_list = []
    for s in lst:
        new_list.append(re.sub('[{}\"\' :uRemoteWallClockTimeUsrp]', '', s))

    return map(float, new_list)

def get_unique_vals(key, hours):
    # get only vals since hours ago
    gt = "$gt"
    secs_ago = str(time.time() - hours * 60 * 60)

    # vals = coll.distinct(key, {'JobStartDate': { gt: secs_ago } } ) - pymongo doesn't support this command :(
    # so I have to do this instead

    vals = coll.find( { 'JobStartDate': { gt: secs_ago } }, { key: 1, '_id': 0 } )

    # removing unecessary characters
    vals = map(str, vals)
    for index, val in enumerate(vals):
        s = re.sub('[\"\'{}]', '', val)
        if key == 'User':
           s = s.split('u', 2)[-1]
           vals[index] = s.split('@', 1)[0]
        elif key == 'LastRemoteHost':
           s = s.split('@', 1)[-1]
           vals[index] = s.split('.', 1)[-1]

    # removing multiple instances of strings and empty strings
    vals = list(set(vals))
    vals = filter(None, vals)

    return vals

def query_items(proj, crit, key):
    rgx = '$regex'

    ret_list = []

    if(key == 'User'):
        crit = '\"' + crit + '\"'
        criteria = { 'User': crit }
    elif(key == 'LastRemoteHost'):
        criteria = { 'LastRemoteHost': crit }

    projection = { proj: 1, '_id': 0 }

    for condor_history in coll.find(criteria, projection):
        ret_list.append(condor_history)

    return map(str, ret_list)

def count_completions(key, name, hours):
    secs_ago = str(time.time() - hours * 60 * 60)
    rgx = "$regex"
    gt = "$gt"
    count = coll.find({ key: { rgx: name }, 'JobStatus': '4', 'JobStartDate': { gt: secs_ago} }).count()
    return count

def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    key = d.get('key', [''])[0]
    item = d.get('item', [''])[0]
    hours = int(d.get('hours', [''])[0])

    unique_vals = get_unique_vals(key, hours)

    response_body = []

    for uv in unique_vals:
        response_body.append( {uv: get_coord(uv)} )
        #response_body.append({ uv: count_completions(key, uv, hours) })
        #response_body.append(get_coord(uv))

    response_headers = [('Content-type', 'application/json')]
    status = '200 OK'

    start_response(status, response_headers)

    return json.dumps(response_body)

# doesnt' seem to work at the moment
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
        import sys

        if environ['.contenttype'] is None:
           start_response('500 Error', [('Content-type', 'text/plain')])
           trace = cgitb.text(sys.exc_info())
        elif environ['.contenttype'].lower() == 'text/html':
           trace = cgitb.html(sys.exc_info())
        else:
           trace = cgitb.text(sys.exc_info())

        return [trace]
    return wrapper

#application = error_capture(application)
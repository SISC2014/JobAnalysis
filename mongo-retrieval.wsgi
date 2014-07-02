'''
Erik Halperin, 07/01/2014

'''
from cgi import parse_qs # for parsing query_strings in url
import re # for character removal from strings
from pymongo import MongoClient # connect to mongodb
import json # output json document
import unicodedata
#from unicodedata import normalize

# connect to database
client = MongoClient('mc.mwt2.org', 27017)
db = client.condor_history
coll = db.history_records

def str_to_num(lst):
    new_list = []
    for s in lst:
        new_list.append(re.sub('[{}\"\' :uRemoteWallClockTimeUsrp]', '', s))

    return map(float, new_list)

def get_unique_vals(key):
    vals = coll.distinct(key)

    for index, val in enumerate(vals):
        s = unicodedata.normalize('NFD', val).encode('ascii', 'ignore')
        vals[index] = re.sub('[\"\']', '', s)

    return vals

def query_items(item, username):
    rgx = '$regex'
    username = '\"' + username + '\"'

    ret_list = []

    criteria = { 'LastRemoteHost': { rgx: '' } }
    criteria = { 'User': username }

    projection = { item: 1, '_id': 0 }

    for condor_history in coll.find(criteria, projection):
        ret_list.append(condor_history)

    return map(str, ret_list)

def application(environ, start_response):
    # parsing query strings
    d = parse_qs(environ['QUERY_STRING'])
    key = d.get('key', [''])[0]
    item = d.get('item', [''])[0]

    unique_vals = get_unique_vals(key)

    response_body = []

    for uv in unique_vals:
        items = str_to_num(query_items(item, uv))
        total = sum(items)
        response_body.append( { uv : total } )

    response_headers = [('Content-type', 'application/json')]
    status = '200 OK'

    start_response(status, response_headers)

    return json.dumps(response_body)

#doesnt' seem to work at the moment
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
'''
Created on Jun 19, 2014

@author: Erik Halperin
'''

import re

from pymongo import MongoClient

#takes a list of dictionaries and returns a list of floats
def parseList(l):       
    l = map(str, l)
            
    newlist = []       
    for k in l:
        newlist.append(re.sub('[RemoteWallClockTimeUsrpu_id\"\'{}: ]', '', k))
                
    newlist = map(float, newlist) 
    
    return list(newlist)

#returns a list of dictionaries 
#item is from list of keys, username: "example@login01.osgconnect.net", cluster: "123456", site: "phys.ucconn.edu", 
#coll: MongoDB collection
#username/cluster/site may be None, in which case they will not be used
#item should be _id
def dbFindItemFromUser(item, username, cluster, site, coll):
    mylist = []
    rgx = "$regex"
    
    if(username != None):
        username = '\"' + username + '\"'
        dicU = {'User': username }
    else:
        dicU = {}
        
    if(cluster != None):
        dicC = { 'ClusterId': cluster }
    else:
        dicC = {}
        
    if(site != None):
        dicS = { 'LastRemoteHost': { rgx: site } }
    else:
        dicS = {}
        
    dicU.update(dicC)
    dicU.update(dicS)

    pr = { item: 1, '_id': 0 }
        
    for condor_history in coll.find(dicU, pr):
        mylist.append(condor_history)
    
    return mylist    

#returns a list of dictionaries
#username and coll are same as above
def dbFindIdFromUser(username, coll):
    mylist = []    
    username = '\"' + username + '\"'
        
    cr = { 'User': username }
    pr = { '_id': 1 }
    
    for condor_history in coll.find(cr, pr):
        mylist.append(condor_history)
    
    return mylist

#returns list of 3 items: list(efficiencies), # of jobs plotted, # of total jobs    
def getEfficiency(username, cluster, site, coll):
    ruc = parseList(dbFindItemFromUser("RemoteUserCpu", username, cluster, site, coll))
    rwct = parseList(dbFindItemFromUser("RemoteWallClockTime", username, cluster, site, coll))
    
    efflist = []
    totcount = 0
    goodcount = 0 #certain efficiency values are >1 due to a condor error. these values are discarded
    zerocount = 0 #testing possible condor bug where RemoteUserCpu is 0 but RemoteWallClockTime is quite large
    
    for x,y in zip(ruc, rwct):
        if(y == 0):
            totcount += 1
        elif(x/y > 1):
            totcount += 1
        else:
            if(x == 0):
                zerocount +=1
            efflist.append(x/y)
            totcount += 1
            goodcount +=1
                
    return [efflist, goodcount, totcount]
    
def main(host, port):
    client = MongoClient(host, port)
    db = client.condor_history
    coll = db.history_records
    


def application(environ, start_response):
	output = []

	response_headers = [('Content-type', 'text/html'), ('Content-Length', str(len(response_body)))]
	status = '200 OK'


main('mc.mwt2.org', 27017)
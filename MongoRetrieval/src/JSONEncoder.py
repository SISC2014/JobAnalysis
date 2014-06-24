'''
Created on Jun 24, 2014

@author: Erik Halperin
'''

import json
from EfficiencyHistogram import *

def plotStuff(vals, heights):
    
    num_vals = []
    counter = 0
    for k in vals:
        num_vals.append(counter)
        counter += 1
    
    plt.bar(num_vals, heights)
    plt.xticks(num_vals, vals)
    plt.show()

def mainJSON(host, port):
    client = MongoClient(host, port)
    db = client.condor_history
    coll = db.history_records
    
    data = [ {} ]
    
    site_list = ["ucdavis.edu", "bnl.gov", "smu.edu", "local", "tier2", "unl.edu", "aglt2.org", "iu.edu", "purdue.edu", "uc.mwt2.org", 
                 "northwestern.edu", "nd.edu", "wisc.edu", "buffalo.edu"]
    days_list = []
    
    for site in site_list:
        time_list = parseList(dbFindItemFromUser("RemoteWallClockTime", None, None, site, coll))
        tot = 0
        for e in time_list:
            tot += e
        days = tot / 60 / 60 / 24
        days_list.append(days)
        dic = {site: days}
        data[0].update(dic)
        
    plotStuff(site_list, days_list)
    
    return(json.dumps(data))
    
mainJSON('mc.mwt2.org', 27017)
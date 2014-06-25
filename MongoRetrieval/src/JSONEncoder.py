'''
Created on Jun 24, 2014

@author: Erik Halperin
'''

import json
from pylab import *
from numpy import *
from pymongo import MongoClient
from EfficiencyHistogram import dbFindItemFromUser, parseList

def plotStuff(vals, heights):
    
    num_vals = []
    counter = 0
    for k in vals:
        num_vals.append(counter)
        counter += 1
    
    plt.bar(num_vals, heights)
    plt.xticks(num_vals, vals)
    plt.show()

def mainJSON(host, port, readfile, writefile):
    client = MongoClient(host, port)
    db = client.condor_history
    coll = db.history_records
    
    data = [ {} ]
    
    f = open(readfile, 'r')
    try:
        site_list = f.read().splitlines()
    finally:
        f.close()
    
    for site in site_list:
        time_list = parseList(dbFindItemFromUser("RemoteWallClockTime", None, None, site, coll))
        tot = 0
        for e in time_list:
            tot += e
        days = tot / 60 / 60 / 24
        dic = {site: days}
        data[0].update(dic)
    
    plotStuff(list(data[0].keys()), list(data[0].values()))
    
    f = open(writefile, 'w')
    try:
        json.dump(data, f)
    finally:
        f.close()
    
mainJSON('mc.mwt2.org', 27017, 'ListOfSites.txt', 'data.json')
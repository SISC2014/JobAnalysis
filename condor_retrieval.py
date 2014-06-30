# Erik Halperin, 6/26/14
# Polls a Condor collector for some data and outputs it to Mongo
# collectors: osg-flock.grid.iu.edu, uc3-mgt.mwt2.org, appcloud.uchicago.edu

import threading, sys, time, argparse
import classad, htcondor
from pymongo import Connection
from concurrent import futures

#set up mongo collection
connection = Connection('mc.mwt2.org', 27017)
db = connection.visualization_db
dbc = db.condor_records

def mongo_store(coll):
    slot_state = coll.query(htcondor.AdTypes.Startd,'true',['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])
    timestamp = str(int(time.time()))

    #storing into mongo
    for slot in slot_states:
        sl = dict(slot)
        if 'JobId' in sl:
            sl['_id'] = sl.pop('JobId')
        else:
            continue
        if 'TargetType' in sl:
            del sl['TargetType']
        if 'CurrentTime' in sl:
            del sl['CurrentTime']
        if 'MyType' in sl:
            del sl['MyType']
        #dbc.insert(sl)
        #print(sl)
    return sl

def main():
    while(1):
        parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and store it in mongo")
        parser.add_argument("collector", help="address of the HTCondor collector")
        parser.add_argument("time", help="time to wait between polling")
        args = parser.parse_args()

        print("Start Time: ", time.time())
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            for sl in executor.map(mongo_store, args.collector):
                print(sl)
        print("End Time: ", time.time())

        time.sleep(float(args.time))
main()

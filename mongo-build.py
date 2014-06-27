# Erik Halperin, 6/26/14
# Polls a Condor collector for some data and outputs it to Mongo
# collectors: osg-flock.grid.iu.edu, uc3.mgt.mwt2.org, appcloud.uchicago.edu

import threading, sys, time, argparse
import classad, htcondor
from pymongo import Connection

class myThread(threading.Thread):
    def __init__(self, coll):
        threading.Thread.__init__(self)
        self.coll = coll
    def run(self):
        slot_state = self.coll.query(htcondor.AdTypes.Startd, "true", ['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])
        self.timestamp = str(int(time.time()))
        
        lst = []

        #indexing yo
        for slot in slot_state:
            sl = dict(slot)
            #sl['_id'] = sl.pop('JobId')
            #del sl['TargetType']
            #del sl['CurrentTime']
            #del sl['MyType']
            lst.append(sl)

        print(lst)

        return


def main():
    #set up mongo collection
    connection = Connection('mc.mwt2.org', 27017)
    db = connection.visualization_db
    dbc = db.condor_records
    while(1):
        parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and store it in mongo")
        parser.add_argument("collector", help="address of the HTCondor collector")
        parser.add_argument("time", help="time to wait between polling")
        args = parser.parse_args()

        #create threads so that 
        thr = myThread(htcondor.Collector(args.collector))
        thr.start()
        time.sleep(float(args.time))
        
main()

#parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and dump into mongo")
#parser.add_argument("collector", help="address of the HTCondor collector")
#parser.add_argument("mongoserver", help="address of the Redis server") 
#args = parser.parse_args()

#coll = htcondor.Collector(args.collector)
#slotState = coll.query(htcondor.AdTypes.Startd, "true",['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])

#db = connection['test-db']
#dbc = db['test-db']

#timestamp = str(int(time.time()))

#design indexing

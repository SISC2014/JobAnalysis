# Erik Halperin, 6/26/14
# Polls a Condor collector for some data and outputs it to Mongo
# collectors: osg-flock.grid.iu.edu, uc3-mgt.mwt2.org, appcloud.uchicago.edu
# Takes ~3 seconds each time


import sys, time, argparse
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

    #storing data into mongo
    for slot in slot_state:
        sl = dict(slot)
        if 'JobId' in sl:
            sl['_id'] = sl.pop('JobId') #JobId will become _id for mongo db
        else:
            continue
        if 'TargetType' in sl: #TargetType, CurrentTime, and MyType tag along for some reason
            del sl['TargetType']
        if 'CurrentTime' in sl:
            del sl['CurrentTime']
        if 'MyType' in sl:
            del sl['MyType']
        dbc.insert(sl)

    return

def main():
    while(1):
        parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and store it in mongo")
        parser.add_argument("time", help="time to wait between polling")
        args = parser.parse_args()

        #not sure if using futures correctly, idea is to run 3 mongo_stores concurrently
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(mongo_store, htcondor.Collector('osg-flock.grid.iu.edu'))
            executor.submit(mongo_store, htcondor.Collector('uc3-mgt.mwt2.org'))
            executor.submit(mongo_store, htcondor.Collector('appcloud.uchicago.edu'))

        time.sleep(float(args.time))

main()


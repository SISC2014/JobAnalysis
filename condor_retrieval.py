# Erik Halperin, 6/26/14
# Polls Condor collectors for some data and outputs it to Mongo
# There must be a config.ini with a [poll] section and the options collectors and intervals, the values of which should be seperated by commas
# Having a hard time precisely counting jobs processsed per second, but generally a couple thousand jobs won't take more than 3 seconds

import sys, time
import classad, htcondor
from pymongo import Connection #to connect to mongo
from concurrent import futures #for processing collectors concurrently
from ConfigParser import RawConfigParser #for reading from config.ini

#set up mongo collection
connection = Connection('mc.mwt2.org', 27017)
db = connection.visualization_db
dbc = db.condor_records

#default wait time interval
def_int = 30

def config_parse():
    conf = RawConfigParser()
    conf.read('config.ini')

    collectors = []
    intervals = []
    if conf.has_section('poll'):
        #get collectors
        if conf.has_option('poll', 'collectors'):
            value = conf.get('poll', 'collectors')
            for v in value.split(','):
                collectors.append(v.strip())

        #get wait intervals for collectors
        if conf.has_option('poll', 'intervals'):
            value = conf.get('poll', 'intervals')
            for v in value.split(','):
                intervals.append(int(v.strip()))

    #errors and checking
    if(len(collectors) != len(intervals)):
        sys.stderr.write('config.ini: each collector must have a wait interval\n')
        sys.exit(0)

    new_intvs = [i if i > 0 else def_int for i in intervals]

    return [collectors, new_intvs]

def mongo_store(coll, intv):
    while(True):
        slot_state = coll.query(htcondor.AdTypes.Startd,'true',['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])
        timestamp = str(int(time.time()))

        #storing data into mongo
        for slot in slot_state:
            sl = dict(slot)
            if 'JobId' in sl:
                sl['_id'] = sl.pop('JobId') #JobId will become _id for mongo
            else:
                continue
            if 'TargetType' in sl: #TargetType, CurrentTime, and MyType tag along for some reason
                del sl['TargetType']
            if 'CurrentTime' in sl:
                del sl['CurrentTime']
            if 'MyType' in sl:
                del sl['MyType']
            dbc.insert(sl)

        time.sleep(interval)
        
def main():
    ret_list = config_parse() #collectors = ret_list[0], intervals = ret_list[1]

    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        for collector, interval in zip(ret_list[0],ret_list[1]):
            executor.submit(mongo_store, htcondor.Collector(collector), interval)

main()


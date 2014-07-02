# Erik Halperin, 6/26/14
# Polls Condor collectors for some data and outputs it to Mongo
# There must be a config.ini with a [poll] section and the options collectors and intervals, the values of which should be seperated by commas
# Having a hard time precisely counting jobs processsed per second, but generally a couple thousand jobs won't take more than 3 seconds
# End program by keyboard interrupt (ctrl-c)

import sys, time, signal
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

#main thread changes this to signal child threads to exit
exit_flag = False

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

    #make sure each collector has an interval
    if(len(collectors) != len(intervals)):
        sys.stderr.write('config.ini: each collector must have a wait interval\n')
        sys.exit(0)

    #make sure each interval is a positive number
    new_intvs = [i if i > 0 else def_int for i in intervals]

    return [collectors, new_intvs]

def mongo_store(coll, intv):
    while(True):
        slot_state = coll.query(htcondor.AdTypes.Startd,'true',['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])
        timestamp = str(int(time.time())) #don't have a use for this yet

        if exit_flag is True:
            sys.exit()

        #storing data into mongo
        for slot in slot_state:
            sl = dict(slot)
            if 'JobId' in sl:
                #JobId will become _id for mongo storage
                sl['_id'] = sl.pop('JobId')
            else:
                continue
            #Remove TargetType, CurrentTime, and MyType
            if 'TargetType' in sl:
                del sl['TargetType']
            if 'CurrentTime' in sl:
                del sl['CurrentTime']
            if 'MyType' in sl:
                del sl['MyType']
            dbc.insert(sl)

        time.sleep(intv)

def main():
    ret_list = config_parse() #collectors = ret_list[0], intervals = ret_list[1]
    global exit_flag

    #process each collector in a thread running concurrently with the other threads
    with futures.ThreadPoolExecutor(max_workers=len(ret_list[1])) as executor:
        for collector, interval in zip(ret_list[0],ret_list[1]):
            executor.submit(mongo_store, htcondor.Collector(collector), interval)
        while True:
            #pause indefinitely, waiting for SIGINT
            try:
                signal.pause()
            except KeyboardInterrupt:
                break
            except:
                pass

        #once out of infinite loop, end child threads
        exit_flag = True
        print('\nWaiting for child threads to exit...')
        sys.exit(0)

main()

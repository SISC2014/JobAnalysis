# Erik Halperin, 6/26/14
# Polls Condor collectors for some data and outputs it to Mongo
# There must be a config.ini with a [poll] section and the options collectors and intervals, the values of which should be seperated by commas
# Having a hard time precisely counting jobs processsed per second, but generally a couple thousand jobs won't take more than 3 seconds
# End program by keyboard interrupt (ctrl-c)

import sys, time, signal
import classad, htcondor # for retrieving from condor collectors
from pymongo import MongoClient # to connect to mongo
from pymongo import database # access mongo database
from pymongo import collection # access mongo collection
from concurrent import futures # for processing collectors concurrently
from ConfigParser import RawConfigParser # for reading from config.ini

# main thread changes this to signal child threads to exit
exit_flag = False

def config_parse():
    conf = RawConfigParser()
    conf.read('config.ini')

    collectors, intervals = [], []
    try:
        # get collectors
        value = conf.get('poll', 'collectors')
        for v in value.split(','):
            collectors.append(v.strip())
        # get wait intervals for collectors
        value = conf.get('poll', 'intervals')
        for v in value.split(','):
            intervals.append(int(v.strip()))
    except Exception:
        sys.stderr.write('config.ini: must specify condor collectors and collector wait intervals\n')
        sys.exit(0)

    classad_list = []
    # get classads
    try:
        value = conf.get('poll', 'classads')
        for v in value.split(','):
            classad_list.append(v.strip())
    except Exception:
        sys.stderr.write('config.ini: must specify classads to be queried\n')
        sys.exit(0)

    try:
        # get collection info
        url = conf.get('mongo', 'url')
        port = int(conf.get('mongo', 'port'))
        db_str = conf.get('mongo', 'database')
        m_coll_str = conf.get('mongo', 'collection')
    except Exception:
        sys.stderr.write('config.ini: must specifiy mongo url, port, database, and collection\n')
        sys.exit(0)

    try:
        # set up mongo collection
        client = MongoClient(url, port)
        db = database.Database(client, db_str)
        # Note: m_coll is mongo collection, coll is condor collector
        m_coll = collection.Collection(db, m_coll_str)
    except Exception, e:
        sys.stderr.write('could not connect to mongo collector, check url and port\n')
        print(str(e))
        sys.exit(0)

    # make sure each collector has an interval
    if(len(collectors) != len(intervals)):
        sys.stderr.write('config.ini: each collector must have a wait interval\n')
        sys.exit(0)

    # make sure each intervals is an integer greater than 0
    for intv in intervals:
        if intv >= 0:
            continue
        else:
            sys.stderr.write('config.ini: each wait interval must be an integer greater than 0')
            sys.exit(0)

    return [collectors, intervals, m_coll, classad_list]

def mongo_store(coll, intv, m_coll, classads):
    while(True):
        if exit_flag is True:
            sys.exit()

        # gets each job running on condor rather than ones created since last poll - this needs to be changed
        slot_state = coll.query(htcondor.AdTypes.Startd, 'true', classads)
        timestamp = str(int(time.time())) # don't have a use for this yet

        # storing data into mongo
        for slot in slot_state:
            sl = dict(slot)
            if 'JobId' in sl:
                # JobId will become _id for mongo storage
                sl['_id'] = sl.pop('JobId')
            else:
                continue

            # The classads TargetType, CurrentTime, and MyType always tag along for some reason
            # CurrentTime yields time() which mongo can't parse, so it must be removed -- we can leave the rest
            if 'CurrentTime' in sl:
                del sl['CurrentTime']
            if 'TargetType' in sl:
                del sl['TargetType']
            if 'MyType' in sl:
                del sl['MyType']

            try:
                # use upsert to update existing jobs in mongo if they're still in the collector
                jobid = sl['_id']
                del sl['_id']
                m_coll.update( { '_id': jobid }, { '$set': sl }, upsert=True)
            except Exception, e:
                print(str(e))
                sys.exit(0)

        time.sleep(intv)

def main():
    ret_list = config_parse() # collectors = ret_list[0], intervals = ret_list[1], collection = ret_list[2], classads = ret_list[3]
    global exit_flag

    # process each collector in a thread running concurrently with the other threads
    with futures.ThreadPoolExecutor(max_workers=len(ret_list[1])) as executor:
        for collector, interval in zip(ret_list[0],ret_list[1]):
            executor.submit(mongo_store, htcondor.Collector(collector), interval, ret_list[2], ret_list[3])
        while True:
            # pause indefinitely, waiting for SIGINT
            try:
                signal.pause()
            except KeyboardInterrupt:
                break
            except Exception:
                pass

        # once out of infinite loop, end child threads
        exit_flag = True
        print('\nWaiting for child threads to exit...')
        sys.exit(0)

main()

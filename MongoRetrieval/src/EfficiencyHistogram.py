'''
Created on Jun 19, 2014

@author: Erik Halperin

List of Keys
_id
JobStartDate
Requirements
TransferInput
TotalSuspensions
LastJobStatus
BufferBlockSize
OrigMaxHosts
RequestMemory
WantRemoteSyscalls
LastHoldReasonCode
ExitStatus
Args
JobFinishedHookDone
JobCurrentStartDate
CompletionDate
JobLeaseDuration
Err
RemoteWallClockTime
JobUniverse
RequestCpus
RemoveReason
StreamErr
Rank
WantRemoteIO
LocalSysCpu
UsedOCWrapper
CumulativeSlotTime
TransferIn
MachineAttrCpus0
CondorPlatform
CurrentTime
ExitReason
StreamOut
WantCheckpoint
GlobalJobId
TransferInputSizeMB
JobStatus
LastPublicClaimId
MemoryUsage
NumSystemHolds
TransferOutput
PeriodicRemove
NumShadowStarts
LastHoldReasonSubCode
LastSuspensionTime
ShouldTransferFiles
QDate
RemoteSysCpu
ImageSize_RAW
LastRemoteHost
CondorVersion
DiskUsage_RAW
PeriodicRelease
NumCkpts_RAW
JobCurrentStartExecutingDate
ProjectName
CoreSize
RemoteUserCpu
BytesSent
Owner
BytesRecvd
ExitCode
NumJobStarts
ExecutableSize_RAW
Notification
ExecutableSize
Environment
StartdPrincipal
RootDir
MinHosts
CumulativeSuspensionTime
JOBGLIDEIN_ResourceName
ProcId
MATCH_EXP_JOBGLIDEIN_ResourceName
OnExitRemove
User
UserLog
CommittedSuspensionTime
NumRestarts
JobCoreDumped
Cmd
NumJobMatches
DiskUsage
LastRemotePool
CommittedSlotTime
ResidentSetSize
WhenToTransferOutput
ExitBySignal
Out
RequestDisk
ImageSize
NumCkpts
LastJobLeaseRenewal
MachineAttrSlotWeight0
ResidentSetSize_RAW
JobPrio
JobRunCount
PeriodicHold
ClusterId
NiceUser
MyType
LocalUserCpu
BufferSize
LastHoldReason
CurrentHosts
LeaveJobInQueue
OnExitHold
EnteredCurrentStatus
MaxHosts
CommittedTime
LastMatchTime
In
JobNotification
'''


from array import *
from pylab import *
from numpy import *
import string
import re

import pymongo
from pymongo import MongoClient

#takes a list of dictionaries and returns a list of strings
def parseList(l):       
    l = map(str, l)
            
    newlist = []       
    for k in l:
        newlist.append(re.sub('[RemoteWallClockTimeUsrpu_id\"\'{}: ]', '', k))
                
    newlist = map(float, newlist) 
    
    return list(newlist)

#returns a list of dictionaries 
#item is from list of keys, username: example@login01.osgconnect.net, cluster: 123456, coll: MongoDB collection
def dbFindItemFromUser(item, username, cluster, coll):
    client = MongoClient('mc.mwt2.org', 27017)
    db = client.condor_history
    coll = db.history_records

    mylist = []
    
    if(username != None):
        username = '\"' + username + '\"'
        if(cluster != None):
            cr = { 'User': username, 'ClusterId': cluster }
        else:
            cr = { 'User': username }
    elif(cluster != None):
        cr = { 'ClusterId': cluster }

    pr = { item: 1, '_id': 0 }
    
    for condor_history in coll.find(cr, pr):
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

#creates a scatterplot of two lists
def plotScatter(lst1, lst2, xlab, ylab, title):
    plt.plot(lst1, lst2, 'bo')
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.title(title)
    plt.show()
    
#creates a histogram of a list
#l: list to plot, bs: number of bins
def plotHist(l, bs, xlab, ylab, title):
    plt.hist(l, bins=bs)
    plt.title(title)
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.show()
    
def main(host, port):
    client = MongoClient(host, port)
    db = client.condor_history
    coll = db.history_records
    
    ruc = parseList(dbFindItemFromUser("RemoteUserCpu", "lfzhao@login01.osgconnect.net", None, coll))
    rwct = parseList(dbFindItemFromUser("RemoteWallClockTime", "lfzhao@login01.osgconnect.net", None, coll))
    cid = parseList(dbFindIdFromUser("lfzhao@login01.osgconnect.net", coll))
    efflist = [x/(y+0.000001) for x,y in zip (ruc, rwct)] #+.0000001 so no divideByZero error
    
    ruc225926 = parseList(dbFindItemFromUser("RemoteUserCpu", "lfzhao@login01.osgconnect.net", "225926", coll))
    rwct225926 = parseList(dbFindItemFromUser("RemoteWallClockTime", "lfzhao@login01.osgconnect.net", "25926", coll))
    el225926 = [x/(y) for x,y in zip (ruc225926, rwct225926)]
    #plotHist(el225926, 50, "UserCPU/WallClockTime", "Frequency", "Efficiencies of Jobs for Cluster 225926")

    #plotScatter(cid, efflist, "Job", "Efficiency", "Efficiency of Each Job")
    #plotHist(efflist, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies of Jobs (42,000 total)")
    
main('mc.mwt2.org', 27017)
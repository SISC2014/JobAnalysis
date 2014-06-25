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

from pylab import *
from numpy import *
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

#creates a scatterplot of two items
def plotScatter(item1, item2, username, cluster, coll, xlab, ylab, title):
    lst1 = parseList(dbFindItemFromUser(item1, username, cluster, coll))
    lst2 = parseList(dbFindItemFromUser(item2, username, cluster, coll))
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
    
#Given at least one input for username/cluster/site, creates a histogram of the RemoteUserCpu/RemoteWallClockTime for the results
def efficiencyHistogram(username, cluster, site, coll, bins, xlab, ylab, title):
    retlist = getEfficiency(username, cluster, site, coll) #0: efflist, 1: goodcount, 2: totcount
    
    print("Jobs Plotted:", retlist[1], "/", retlist[2])
    plotHist(retlist[0], bins, xlab, ylab, title)
    
def fourEffHists(lst1, lst2, lst3, lst4, lab1, lab2, lab3, lab4, bs, xlab, ylab, title):
    plt.hist(lst1, bins=bs, histtype='stepfilled', label=lab1)
    plt.hist(lst2, bins=bs, histtype='stepfilled', label=lab2)
    plt.hist(lst3, bins=bs, histtype='stepfilled', label=lab3)
    plt.hist(lst4, bins=bs, histtype='stepfilled', label=lab4)
    plt.title(title)
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.legend()
    plt.show()
    
def mainEH(host, port):
    client = MongoClient(host, port)
    db = client.condor_history
    coll = db.history_records
    
    str_uc = "uc.mwt2.org"
    str_uconn = "phys.uconn.edu"
    str_smu = "hpc.smu.edu"
    str_bnl = "usatlas.bnl.gov"
    str_lfz = "lfzhao@login01.osgconnect.net"
    
    #lst1 = getEfficiency(None, None, str_uc, coll)
    #lst2 = getEfficiency(None, None, str_uconn, coll)
    #lst3 = getEfficiency(None, None, str_smu, coll)
    #lst4 = getEfficiency(None, None, str_bnl, coll)
    
    #doesn't look very good cause of large range of data
    #fourEffHists(lst1,lst2,lst3,lst4, str_uc, str_uconn, str_smu, str_bnl, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies for Four Sites")
    
    #efficiencyHistogram(None, None, None, coll, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies for All Jobs (95,251 jobs)")
    #efficiencyHistogram(None, None, str_uc, coll, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies for uc.mwt2.org")
    #efficiencyHistogram(None, None, str_uconn, coll, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies for phys.uconn.edu")
    #efficiencyHistogram(None, None, str_smu, coll, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies for hpc.smu.edu")
    efficiencyHistogram(None, None, str_bnl, coll, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies for usatlas.bnl.gov")
    #efficiencyHistogram(str_lfz, None, None, coll, 100, "UserCPU/WallClockTime", "Frequency", "Efficiencies of lfzhao")
        
mainEH('mc.mwt2.org', 27017)
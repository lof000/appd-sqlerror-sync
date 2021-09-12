import json
import requests
import os
  
_APPD_CONTROLER_URL=None
_APPD_CONTROLLER_PORT=None
_APPD_EUM_PORT=None
_APPD_API_TOKEN=None
_APPD_ANALYTICS_API_TOKEN=None
_APPD_SNAPSHOT_PERIODS_MIN=None
_APPD_GLOBAL_ACCOUNT_NAME=None 
_APPD_APPLICATION_NAME=None 


def schemeExists():
    controllerUrl = _APPD_CONTROLER_URL+':'+_APPD_EUM_PORT
    headers = {'X-Events-API-AccountName': _APPD_GLOBAL_ACCOUNT_NAME,'X-Events-API-Key':_APPD_ANALYTICS_API_TOKEN ,'Content-type':'application/vnd.appd.events+json;v=2'}
    r = requests.get(controllerUrl+'/events/schema/sqlerror',headers=headers)
    if r.status_code==200:
        return True
    else:
        return False

def createSchema():
    print('creating schema..... ')
    controllerUrl = _APPD_CONTROLER_URL+':'+_APPD_EUM_PORT
    headers = {'X-Events-API-AccountName': _APPD_GLOBAL_ACCOUNT_NAME,'X-Events-API-Key':_APPD_ANALYTICS_API_TOKEN ,'Content-type':'application/vnd.appd.events+json;v=2'}
    newSchema = {
    "schema":{
        "application" : "string",
        "stmt":"string",
        "errorDetails":"string",
        "callingMethod":"string",
        "timestamp":"date",
        "requestGUID":"string",
        "timeTakenInMilliSecs":"integer",
        "localStartTime":"date",
        "serverStartTime":"date"
        }
    }
    r = requests.post(controllerUrl+'/events/schema/sqlerror',headers=headers,json=newSchema)
    return r


def getSqlErrosFromSnapShot(snapshotPayload):
    print('processing... '+str(len(snapshotPayload))+' snapshots')
    onlyJDBC = []

    #filter only snapshotExitCalls - where sql is
    exitCalls = [x for x in snapshotPayload if len(x['snapshotExitCalls']) >0]

    #go through snapshotExitCalls and gather only JDBC exitPoints
    for exitsOnly in exitCalls:
        for snaps in exitsOnly['snapshotExitCalls']:
            if snaps['exitPointName'] == 'JDBC':
                #get all data
                item = {
                    "application" : _APPD_APPLICATION_NAME,
                    "stmt": snaps['detailString'],
                    "errorDetails": snaps['errorDetails'],
                    "callingMethod": snaps['callingMethod'],
                    "timestamp": snaps['timestamp'],
                    "requestGUID": exitsOnly['requestGUID'],
                    "timeTakenInMilliSecs": exitsOnly['timeTakenInMilliSecs'],
                    "localStartTime": exitsOnly['localStartTime'],
                    "serverStartTime": exitsOnly['serverStartTime'],
                }
                onlyJDBC.append(item)
    return onlyJDBC

def getSnapshotsFromJsonFile():
    f = open('snap.json',)
    data = json.load(f)
    f.close()
    return data

def getSnapshotsFromAppd():
    headers = {'Authorization': 'Bearer '+_APPD_API_TOKEN,'Content-type':'application/vnd.appd.events+json;v=2'}

    params = {'time-range-type': 'BEFORE_NOW',
    'duration-in-mins':str(_APPD_SNAPSHOT_PERIODS_MIN),
    'need-props':'true',
    'need-exit-calls':'true',
    'error-occurred':'true',
    'output':'json'}

    controllerUrl = _APPD_CONTROLER_URL+':'+_APPD_CONTROLLER_PORT

    r = requests.get(controllerUrl+'/controller/rest/applications/'+_APPD_APPLICATION_NAME+'/request-snapshots',headers=headers,params=params)
    return json.loads(r.text)



def sendSqlErrorsToAppd(erroList):
    print('Sending '+str(len(erroList))+' snapshots')
    controllerUrl = _APPD_CONTROLER_URL+':'+_APPD_EUM_PORT
    headers = {'X-Events-API-AccountName': _APPD_GLOBAL_ACCOUNT_NAME,'X-Events-API-Key':_APPD_ANALYTICS_API_TOKEN ,'Content-type':'application/vnd.appd.events+json;v=2'}
    r = requests.post(controllerUrl+'/events/publish/sqlerror',headers=headers,json = erroList)
    return r


def getEventsFromAppD(requestGUIDs):
    print('Checking if requestGUIDs are already there: '+str(len(requestGUIDs))+'')

    inSelect = "','".join(map(str,requestGUIDs))

    qry = [{
	"query" : "SELECT requestGUID FROM sqlerror WHERE requestGUID IN ('"+inSelect+"')",
	"mode":"none"}]
    controllerUrl = _APPD_CONTROLER_URL+':'+_APPD_EUM_PORT
    headers = {'X-Events-API-AccountName': _APPD_GLOBAL_ACCOUNT_NAME,'X-Events-API-Key':_APPD_ANALYTICS_API_TOKEN,'Content-type':'application/vnd.appd.events+json;v=2'}
    r = requests.post(controllerUrl+'/events/query',headers=headers,json = qry)
   
    responseFromAppd = json.loads(r.text)

    jatem = set()
    for rst in responseFromAppd[0]['results']:
        jatem.add(str(rst[0]))

    return jatem


def requestGUIDs(errors):
    reqs=[]
    for item in errors:
        reqs.append(item['requestGUID'])
    return reqs


def filterSnaps(errors,already):
    filtered = []
    for err in errors:
        if not (err['requestGUID'] in already):
            filtered.append(err)
    return filtered


def process():
    #data = getSnapshotsFromJsonFile()

    #get snapshots from appD
    data = getSnapshotsFromAppd()

    #filter only SQL erros
    errorsSQLSnapshots = getSqlErrosFromSnapShot(data)

    #check what already exists
    alreadyThere = getEventsFromAppD(requestGUIDs(errorsSQLSnapshots))

    filtered = filterSnaps(errorsSQLSnapshots,alreadyThere)

    #send errors to AppD
    if (len(filtered)>0):
        print("Sending to SQL Errors to Appd...")
        sentToAppD = sendSqlErrorsToAppd(filtered)
        print("Sent.. return code:"+ str(sentToAppD.status_code))
    else:
        print("Nothing to send to AppD")


def validateEnv():
    if not _APPD_CONTROLER_URL:
        print(" APPD_CONTROLER_URL MISSING")
        return False

    if not _APPD_CONTROLLER_PORT:
        print(" APPD_CONTROLLER_PORT MISSING")
        return False
    
    if not _APPD_EUM_PORT:
        print(" APPD_EUM_PORT MISSING")
        return False
    
    if not _APPD_API_TOKEN:
        print(" APPD_API_TOKEN MISSING")
        return False

    if not _APPD_ANALYTICS_API_TOKEN:
        print(" APPD_ANALYTICS_API_TOKEN MISSING")
        return False

    if not _APPD_SNAPSHOT_PERIODS_MIN:
        print(" APPD_SNAPSHOT_PERIODS_MIN MISSING")
        return False

    if not _APPD_GLOBAL_ACCOUNT_NAME:
        print(" APPD_GLOBAL_ACCOUNT_NAME MISSING")
        return False

    if not _APPD_APPLICATION_NAME:
        print(" APPD_APPLICATION_NAME MISSING")
        return False       

    return True

_APPD_CONTROLER_URL=os.environ.get('APPD_CONTROLER_URL')
_APPD_CONTROLLER_PORT=os.environ.get('APPD_CONTROLLER_PORT')
_APPD_EUM_PORT=os.environ.get('APPD_EUM_PORT')
_APPD_API_TOKEN=os.environ.get('APPD_API_TOKEN')
_APPD_ANALYTICS_API_TOKEN=os.environ.get('APPD_ANALYTICS_API_TOKEN')
_APPD_SNAPSHOT_PERIODS_MIN=os.environ.get('APPD_SNAPSHOT_PERIODS_MIN')
_APPD_GLOBAL_ACCOUNT_NAME=os.environ.get('APPD_GLOBAL_ACCOUNT_NAME')
_APPD_APPLICATION_NAME=os.environ.get('APPD_APPLICATION_NAME')

go = False

if (validateEnv()):
    if not schemeExists():
        print("Schema not found!")
        rs = createSchema()
        if (rs.status_code!=201):
            print('Error Creating schema')
            print(rs.status_code)
            print(rs.text)
        else:
            print('Schema created..')
            go=True
    else:
        print('Schema found..')
        go=True
        
    if(go):    
        process()
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.background import BackgroundTask
from starlette.endpoints import HTTPEndpoint
from starlette.requests import Request
import uvicorn
import grequests
import logging
import os
import json
import random
import kvstorage
import views
import shard
import hashlib
import zlib
import math
import jsonpickle

# Setup
logging.basicConfig(level=logging.DEBUG)
app = Starlette(debug=True)
#keyCrc = zlib.crc32()



# Constants
BASE = 'http://'
KVS_ENDPOINT = '/key-value-store/'
VIEW_ENDPOINT = '/key-value-store-view/'
SHARD_ENDPOINT = '/key-value-store-shard/'
OWN_SOCKET = os.environ['SOCKET_ADDRESS']
if 'SHARD_COUNT' in os.environ:
    shard_count = os.environ['SHARD_COUNT']
else:
    shard_count = None
TIMEOUT_TIME = 3
MAX_VERSION = 9223372036854775806

# Process-specific constants to help set up and record itself within it's assigned shard.
shardIDs = ""
view = views.ViewList(os.environ['VIEW'], OWN_SOCKET)
groupList = []
pendingRequests = []
idList = []
ip = zlib.crc32(OWN_SOCKET.encode('utf-8'))

new_native = 0
native_shard_id = 0
pendingRequests = []


def balance(index, fullList):
    global groupList
    # special:: if replica group has 0
    # if a certain replica group N_i has greater than 2
    # we move it to the replica group that has less than 2
    logging.debug("Balancing about to take place.")
    for i, group in enumerate(fullList):
        if i != index and len(fullList[index].shard_id_members) < 2:
            if len(group.shard_id_members) > 2:
                logging.debug("Current Group: %s", group.getReplicas())
                temp = group.shard_id_members.pop()
                logging.debug("Temp address popped: %s", temp)
                logging.debug("Current Group After Pop: %s",
                              group.getReplicas())
                logging.debug("Starving Group Before Append: %s",
                              fullList[index].getReplicas())
                fullList[index].shard_id_members.append(temp)
                logging.debug("Starving Group After Append: %s",
                              fullList[index].getReplicas())


async def forwarding(key, vs, isFromClient, reqType):
    if isFromClient:
        logging.debug("putforwarding at: Key: %s ReqType: %s View: %s",
                      key, reqType, groupList[native_shard_id].getMembers())
        if reqType == "PUT":
            alist = groupList[native_shard_id].getMembers().copy()
            if OWN_SOCKET in alist:
                alist.remove(OWN_SOCKET)
            rs = (grequests.put(BASE + address + KVS_ENDPOINT + key,
                                json={'value': vs.getValue(),
                                      'version': vs.getVersion(),
                                      'causal-metadata': vs.causalMetadata}) for address in alist)
        elif reqType == "DELETE":
            alist = groupList[native_shard_id].getMembers().copy()
            if OWN_SOCKET in alist:
                alist.remove(OWN_SOCKET)
            rs = (grequests.delete(BASE + address + KVS_ENDPOINT + key,
                                   json={'version': vs.getVersion(),
                                         'causal-metadata': vs.causalMetadata}) for address in alist)
        elif reqType == "VIEW_DELETE":
            rs = (grequests.delete(BASE + address + VIEW_ENDPOINT,
                                   json={'socket-address': vs}) for address in view)
        elif reqType == "VIEW_ADD":
            rs = (grequests.put(BASE + address + VIEW_ENDPOINT,
                                json={'socket-address': vs}) for address in view)
        elif reqType == "HISTORY":
            rs = (grequests.put(BASE + address + "/history/",
                                json={'history': vs,
                                      'shard-id': native_shard_id,
                                      'key-count': len(kvstorage.kvs)}) for address in view)
        else:
            logging.error("forwarding reqType invalid!!!")
        grequests.map(rs, exception_handler=exception_handler,
                      gtimeout=TIMEOUT_TIME)
        logging.debug("forwarding Finished")
    else:
        logging.debug(
            "request is from a replica (not a client), not forwardinging")


if shard_count is not None:
    native_shard_id = 0
    logging.debug("we have a shard count.")
    logging.debug("Chord is being initialized. Shard count: %s", shard_count)

    # Statically handle the first two nodes we'll always need.
    for i in range(int(shard_count)):
        group = "group" + str(i)
        tempGroupID = zlib.crc32(group.encode('utf-8'), 0)
        logging.debug("Group "+str(i)+" hashed to "+str(tempGroupID))
        tempGroup = shard.ReplicaGroup(i, tempGroupID, 0, [], 0, {})
        idList.append(str(i))
        groupList.append(tempGroup)

    shardIDs = ",".join(idList)

    #groupList[tempGroupID].addGroupMember(OWN_SOCKET)
    #logging.debug("Identifier for "+OWN_SOCKET + "= " + str(ipSHA.hexdigest()))
    #hashedGroupID = (int(ipSHA.hexdigest(), 16) % 2) + 1
    #logging.debug(OWN_SOCKET + " will be in replica group: " + str(hashedGroupID))
    #logging.debug(addr + " is going to group " + str(hashedGroupID))
    us = 0
    logging.debug(view)
    for other in view:
        hasher = zlib.crc32(other.encode('utf-8'))
        large = 1

        for i in range(len(groupList)):
            if hasher < groupList[i].getHashID():
                    groupList[i].addGroupMember(other)
                    large = 0
                    break
        if large == 1:
            logging.debug("hashed "+other + "is too large.")
            groupList[0].addGroupMember(other)

    large = 1
    hasher = zlib.crc32(OWN_SOCKET.encode('utf-8'))
    for i in range(len(groupList)):
        if hasher < groupList[i].getHashID():
                groupList[i].addGroupMember(OWN_SOCKET)
                native_shard_id = groupList[i].getShardID()
                large = 0
                break
    if large == 1:
        logging.debug("hashed "+other + "is too large.")
        groupList[0].addGroupMember(OWN_SOCKET)
        native_shard_id = groupList[0].getShardID()

    memList = groupList[native_shard_id].shard_id_members
    memList.sort()
  
else:
    native_shard_id = 0
    logging.debug("this is a new process. retrieving shard_count from an active replica.")
    informant = view[0]
    ad = BASE+informant+'/inform/'
    logging.debug("attempting to reach "+ ad+" for the shard_count.")
    shard_num = grequests.get(ad,timeout = TIMEOUT_TIME)
    numList = grequests.map([shard_num])
    #data = shard_num['shard-count']
    logging.debug("%s",numList)
    shard_count = numList[0].json()['shard-count']
    logging.debug("here is the shard count:"+shard_count+". going to init the system now.")
    logging.debug("Chord is being initialized. Shard count: %s", shard_count)

    # Statically handle the first two nodes we'll always need.
    for i in range(int(shard_count)):
        group = "group" + str(i)
        tempGroupID = zlib.crc32(group.encode('utf-8'), 0)
        logging.debug("Group "+str(i)+" hashed to "+str(tempGroupID))
        tempGroup = shard.ReplicaGroup(i, tempGroupID, 0, [], 0, {})
        idList.append(str(i))
        groupList.append(tempGroup)

    shardIDs = ",".join(idList)

    #groupList[tempGroupID].addGroupMember(OWN_SOCKET)
    #logging.debug("Identifier for "+OWN_SOCKET + "= " + str(ipSHA.hexdigest()))
    #hashedGroupID = (int(ipSHA.hexdigest(), 16) % 2) + 1
    #logging.debug(OWN_SOCKET + " will be in replica group: " + str(hashedGroupID))
    #logging.debug(addr + " is going to group " + str(hashedGroupID))
    us = 0
    logging.debug(view)
    for other in view:
        hasher = zlib.crc32(other.encode('utf-8'))
        large = 1

        for i in range(len(groupList)):
            if hasher < groupList[i].getHashID():
                    groupList[i].addGroupMember(other)
                    large = 0
                    break
        if large == 1:
            logging.debug("hashed "+other + "is too large.")
            groupList[0].addGroupMember(other)

    large = 1
    hasher = zlib.crc32(OWN_SOCKET.encode('utf-8'))

    groupList.sort(key=lambda x: x.hash_id, reverse=False)
    rs = (grequests.put(BASE + address + VIEW_ENDPOINT,
                        json={'socket-address': OWN_SOCKET}) for address in view)
    grequests.map(rs)
    logging.debug("Added ourselves to everyone's view.")
    
#
@app.route('/key-value-store/{key}')
class KeyValueStore(HTTPEndpoint):
    # Put handler for KeyValueStore endpoint
    #   Loads data from reuqest
    #   Sets tasks to
    #        Store value in key - (Await)
    #        forwarding changes if message is from a client - (Background)
    #   Returns to the client
    async def put(self, request):
        # First we set up our variables
        #   key: requested key
        #   value: requested value to add/update
        #   version: unique id for the funtion; used to ensure causality
        #   causalMetadata: list of versions that must be completed before
        #                           value is added to the key
        global groupList
        global pendingRequests
        data = await request.json()
        
        key = request.path_params['key']
        value = ""
        version = ""
        causalMetadata = []
        keyCrc = zlib.crc32(key.encode('utf-8'))
        logging.debug(key + " hashed to " + str(keyCrc))
        logging.debug("Looking up replica group for " + str(keyCrc))

        if len(key) > 50:  # key
            message = {"error": "Key is too long",
                       "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'value' in data:  # value
            value = data['value']
        else:
            message = {"error": "Value is missing", "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'causal-metadata' in data:  # causalMetadata
            causalMetadata = data['causal-metadata']
            if causalMetadata == "":  # Case empty string is passed
                causalMetadata = []
        if 'version' in data:  # version
                        version = data['version']
        else:
            version = random.randint(0, MAX_VERSION)
            logging.info("No Version in data, generating a unique version id: %s", version)

        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s",
                    senderSocket, view, senderSocket in view)
        isFromClient = senderSocket not in groupList[native_shard_id].getMembers()
        logging.debug("===Put at %s : %s", key, value)


        # the key in the request didn't hash into our group id, we have to redirect 
        # it to the proper replica group rather than process it ourselves.
        # we also are going to be responsible for sending the repsone back 
        # to the client.
        for i in range(0,len(groupList)-1):
            logging.debug("i = %s gList[i].hash_id: %s, gList[i+1].hash_id:%s",i,groupList[i].getHashID(),groupList[i+1].getHashID())
            
            if keyCrc > groupList[i].hash_id and keyCrc < groupList[i+1].hash_id:
                logging.debug("found spot for key between %s and %s",i,i+1)
                if native_shard_id != int(groupList[i+1].getShardID()):
                    #forward to other shard group
                     resp = forwardToShard(groupList[i+1].getShardID(),key,data,"PUT")
                     return JSONResponse(resp.json(),status_code=resp.status_code,media_type='application/json')
                    # the key hashed out to the proper group id, process and forward like usual.
                else:
                    logging.debug("key is in correct shard. doing normal operations.")
            elif keyCrc > groupList[-1].getHashID() or keyCrc < groupList[0].getHashID():
                logging.debug("printing shit out bro!!!!!!our shard %s group0shard: %s",native_shard_id,groupList[0].getShardID())
                if int(groupList[0].getShardID()) == native_shard_id:
                    logging.debug("forwarding within our group")
                                      
                else:
                    logging.debug("forwarding elsewhere")
                    resp = forwardToShard(groupList[0].getShardID(),key,data,"PUT")
                    return JSONResponse(resp.json(),status_code=resp.status_code,media_type='application/json')
            
        # Second, we set up the task that will update the value,
        # add the request to the store incase a copy is made while pending,
        # and, set up the forwardinging that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        
        vs = kvstorage.ValueStore(value, version, causalMetadata.copy())
        req = (key, vs)
        pendingRequests.append(req)
        isUpdating = await kvstorage.dataMgmt(key, vs)

        pendingRequests.remove(req)
        # if not isUpdating:
        #     groupList[native_shard_id].incrementKeyCount()
        logging.debug("forwarding history: %s", version)
        await forwarding(None, version, True, reqType="HISTORY" )
        task = BackgroundTask( forwarding, key=key, vs=vs, isFromClient=isFromClient, reqType="PUT")

        # Finally we return
        causalMetadata.append(version)

        if isUpdating:
            message = {
                "message": "Updated successfully",
                "version": version,
                "causal-metadata": causalMetadata,
                "shard-id": jsonpickle.encode(native_shard_id)
                    }
            return JSONResponse(message,
                                status_code=200,
                                background=task,
                                media_type='application/json')
        else:
            message = {
                "message": "Added successfully",
                "version": version,
                "causal-metadata": causalMetadata,
                "shard-id": jsonpickle.encode(native_shard_id)
            }
            return JSONResponse(message,
                                status_code=201,
                                background=task,
                                media_type='application/json')


    async def delete(self, request):
        # First we set up our variables
        #   key: requested key
        #   value: requested value to add/update
        #   version: unique id for the funtion; used to ensure causality
        #   causalMetadata: list of versions that must be completed before
        #                           value is added to the key

        try:
            data = await request.json()
        except:
            data = ""

        key = request.path_params['key']
        version = ""
        causalMetadata = []

        keyCrc = zlib.crc32(key.encode('utf-8'))
        logging.debug(key + " hashed to " + str(keyCrc))


        if len(key) > 50:  # key
            message = {"error": "Key is too long",
                       "message": "Error in DELETE"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'causal-metadata' in data:  # causalMetadata
            causalMetadata = data['causal-metadata']
            if causalMetadata == "":  # Case empty string is passed
                causalMetadata = []
        if 'version' in data:  # version
            version = data['version']
        else:
            version = random.randint(0, MAX_VERSION)
            logging.info(
                "No Version in data, generating a unique version id: %s", version)
        
        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s",
                    senderSocket, view, senderSocket in view)
        isFromClient = senderSocket not in groupList[native_shard_id].getMembers()
        
        logging.debug("===Delete at %s", key)
        
        # the key in the request didn't hash into our group id, we have to redirect 
        # it to the proper replica group rather than process it ourselves.
        # we also are going to be responsible for sending the repsone back 
        # to the client.
        for i in range(0,len(groupList)-1):
            logging.debug("i = %s gList[i].hash_id: %s, gList[i+1].hash_id:%s",i,groupList[i].getHashID(),groupList[i+1].getHashID())
            if keyCrc > groupList[i].hash_id and keyCrc < groupList[i+1].hash_id:
                logging.debug("found spot for key between %s and %s",i,i+1)
                if native_shard_id != int(groupList[i+1].getShardID()):
                    resp = forwardToShard(groupList[i+1].getShardID(),key,data,"DELETE")
                    return JSONResponse(resp.json(),status_code=resp.status_code,media_type='application/json')
                else:
                    logging.debug("key is in correct shard. doing normal operations.")
                    break
            elif keyCrc > groupList[-1].getHashID() or keyCrc < groupList[0].getHashID():
                if int(groupList[0].getShardID()) == native_shard_id:
                    logging.debug("forwarding within our group")
                    break
                                      
                else:
                    logging.debug("forwarding elsewhere")
                    resp = forwardToShard(groupList[0].getShardID(),key,data,"DELETE")
                    return JSONResponse(resp.json(),status_code=resp.status_code,media_type='application/json')


        # Second, we set up the task that will update the value,
        # and the forwarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = kvstorage.ValueStore(None, version, causalMetadata.copy())
        req = (key, vs)
        pendingRequests.append(req)
        isDeleting = await kvstorage.dataMgmt(key, vs)
        if not isDeleting:
            groupList[native_shard_id].decrementKeyCount()
        pendingRequests.remove(req)
        logging.debug("forwarding history: %s", version)
        await forwarding(None, version, True, reqType="HISTORY" )
        task = BackgroundTask( forwarding, key=key, vs=vs, isFromClient=isFromClient, reqType="DELETE")

        # Finally we return
        causalMetadata.append(version)
        if isDeleting:
            message = {
                "message": "Deleted successfully",
                "version": version,
                "causal-metadata": causalMetadata,
                "shard-id": jsonpickle.encode(native_shard_id)
            }
            return JSONResponse(message,
                                status_code=200,
                                background=task,
                                media_type='application/json')
        else:
            message = {
                "message": "Error in DELETE",
                "error": "Key does not exist"
            }
            return JSONResponse(message,
                                status_code=404,
                                media_type='application/json')

    # Get Handler for key value store endpoint
    #   returns to the client with the current state of the key
    async def get(self, request):
        key = request.path_params['key']
        keyCrc = zlib.crc32(key.encode('utf-8'))
        data = {}
        for i in range(0,len(groupList)-1):
            logging.debug("i = %s gList[i].hash_id: %s, gList[i+1].hash_id:%s",i,groupList[i].getHashID(),groupList[i+1].getHashID())
            
            if keyCrc > groupList[i].hash_id and keyCrc < groupList[i+1].hash_id:
                logging.debug("found spot for key between %s and %s",i,i+1)
                if native_shard_id != int(groupList[i+1].getShardID()):
                    #forward to other shard group
                    resp = forwardToShard(groupList[i+1].getShardID(),key,data,"GET")
                    return JSONResponse(resp.json(),status_code=resp.status_code,media_type='application/json')
                    # the key hashed out to the proper group id, process and forward like usual.
                else:
                    logging.debug("return value")
                    break
            elif keyCrc > groupList[-1].getHashID() or keyCrc < groupList[0].getHashID():
                logging.debug("printing shit out bro!!!!!!our shard %s group0shard: %s",native_shard_id,groupList[0].getShardID())
                if int(groupList[0].getShardID()) == native_shard_id:
                    logging.debug("return value") 
                    break                                       
                else:
                    logging.debug("forwarding elsewhere")
                    resp = forwardToShard(groupList[0].getShardID(),key,data,"GET")
                    return JSONResponse(resp.json(),status_code=resp.status_code,media_type='application/json')
        if key in kvstorage.kvs:
            # TODO: Import kvs properly
            vs = kvstorage.kvs[key]
            message = {
                "message": "Retrieved successfully",
                "value": vs.getValue(),
                "version": vs.getVersion(),
                "causal-metadata": vs.causalMetadata
            }
            return JSONResponse(message, status_code=200, media_type='application/json')
        else:
            message = {
                "error": "Key does not exist",
                "message": "Error in GET"}
            return JSONResponse(message, status_code=404, media_type='application/json')


@app.route('/key-value-store-view/')
class KVSView(HTTPEndpoint):
    async def get(self, request):
        message = {"message": "View retreived successfully",
                   "view": repr(view)}
        return JSONResponse(message, status_code=200, media_type='application/json')

    async def put(self, request):
        # Retreive the new address
        # if the data is empty, error
        # if the data already exists in our view, error
        data = await request.json()
        if 'socket-address' in data:
            newAddress = data['socket-address']
        else:
            message = {"error": "Value is missing",
                       "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')

        if newAddress in view:
            message = {"error": "Socket address already exists in the view",
                       "message": "Error in PUT"}
            return JSONResponse(message, status_code=404, media_type='application/json')

        view.add(newAddress)

        message = {"message": "Replica added successfully to the view"}
        return JSONResponse(message, status_code=200, media_type='application/json')

    async def delete(self, request):
        # Retreive the address to delete
        # If the data is empty, error

        data = await request.json()
        if 'socket-address' in data:
            delAddress = data['socket-address']
        else:
            # Case socket address is not in data
            message = {"error": "Value is missing",
                       "message": "Error in DELETE"}
            return JSONResponse(message, status_code=400, media_type='application/json')

        # Check if socket address does exist, delete and return
        if delAddress in view:
            # Delete
            view.remove(delAddress)
            rg = getRepGroup(ip)
            if rg:
                rg.shard_id_members.remove(ip)
            else:
                logging.error("deleteReplica could not find given ip: %s in replica group", ip)

            # Return
            message = {"message": "Replica deleted successfully from the view"}
            return JSONResponse(message, status_code=200, media_type='application/json')

        # Else socket is not in view, return error
        message = {"error": "Socket address does not exist in the view",
                   "message": "Error in DELETE"}
        return JSONResponse(message, status_code=404, media_type='application/json')


@app.route('/store/')
async def store(request):
    tView = list(view)
    tView.append(view.ownSocket)
    message = {
        "kvs": jsonpickle.encode(kvstorage.kvs),
        "history": jsonpickle.encode(kvstorage.history),
        "view": jsonpickle.encode(tView),
        "shard-ids": jsonpickle.encode(groupList),
        "shard-count": jsonpickle.encode(shard_count),
        "group-list": jsonpickle.encode(groupList),
        "pending": jsonpickle.encode(pendingRequests)
    }

    return JSONResponse(message, status_code=200, media_type='application/json')
    # TODO: Check for pending requests

# if request should be forwardinged,
# forwardings PUT at (key, vs) to all
# replicas in view
@app.route('/key-value-store-shard/shard-ids')
def getShardIds(self):
    message = {
        "message": "Shard IDs retrieved successfully",
        "shard-ids": shardIDs}

    return JSONResponse(message, status_code=200, media_type='application/json')

# Return the Replica Group ID that this process belongs to.



@app.route('/key-value-store-shard/node-shard-id')
class ShardMembers(HTTPEndpoint):
    async def get(self,request):
        logging.debug("Shard ID to be returned: %s",native_shard_id)
        message = {
            "message": "Shard ID of the node retrieved successfully", 
            "shard-id": jsonpickle.encode(int(native_shard_id))}
        return JSONResponse(message,status_code=200,media_type='application/json')
    

@app.route('/key-value-store-shard/shard-id-members/{shard}')
class Members(HTTPEndpoint):
    async def get(self,request):
        shard = request.path_params['shard']
        logging.debug("ID requested: %s", shard)
        group = groupList[int(shard)]
        groupString = group.getReplicas()
        logging.debug("Heres our members that will be returned: %s",groupString)
        message = {"message": "Members of shard ID retrieved successfully", 
        "shard-id-members":groupString}
        return JSONResponse(message,status_code=200,media_type='application/json')


@app.route('/key-value-store-shard/shard-id-key-count/{shard}')
class KeyCount(HTTPEndpoint):
    async def get(self,request):
        shard = int(request.path_params['shard'])
        logging.debug("returning shard key count of %s",groupList[shard].getCountOfKeys())
        message = {"message": "Key count of shard ID retrieved successfully",
                "shard-id-key-count": groupList[shard].getCountOfKeys()}
        return JSONResponse(message,status_code=200,media_type='application/json')

@app.route('/history/')
class Hist(HTTPEndpoint):
    async def put(self,request):
        global kvstorage
        data = await request.json()
        if 'history' in data:
            newHistory = data['history']
            shard = data['shard-id']
            keyCnt = data['key-count']
            groupList[int(shard)].key_count = keyCnt
            if newHistory not in kvstorage.history:
                kvstorage.history.append(newHistory)
                message = {"message": "History updated"}
            else:
                message = {"message": "version already in history"}                
        else:
            message = {"message": "No history passed"}
        return JSONResponse(message,status_code=200,media_type='application/json')


@app.route('/key-value-store-shard/add-member/{id}')
class AddReplica(HTTPEndpoint):
    async def put(self,request):
        senderSocket = request.client.host + ":8080"
        data = await request.json()
        destShard = request.path_params['id']      
        if 'socket-address' in data:
            newAddress = data['socket-address']
        else:
            message = {"error": "No address given.",
                       "message": "Error in DELETE"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        rs = (grequests.put(BASE + newAddress+'/id-correct/'+str(destShard),
                           ))
        grequests.map([rs])
        groupList[int(destShard)].addGroupMember(newAddress)
        logging.debug("Added "+str(newAddress) + " to group: " +
                      str(groupList[int(destShard)].getShardID()))
        rs = (grequests.put(BASE + address+ '/member-list/'+str(destShard),
                            json={'socket-address': newAddress})for address in view)
        grequests.map(rs)

        rs = (grequests.put(BASE + '/key-value-store-shard/add-member/'+str(destShard),
                            json={'socket-address':newAddress})for address in view)
        grequests.map(rs)

    
        logging.debug("New members list for replica group " + str(groupList[int(destShard)].getShardID()) + ": "+ str(groupList[int(destShard)].getReplicas()))
        logging.debug(view)
        message = {"message": "Replica added to shard succesfully."}
        return JSONResponse(message,status_code=200,media_type='application/json')

@app.route('/inform/')
class Inf(HTTPEndpoint):
    def get(self,request):
        message = {"message": "here is your shard-id.",
                    "shard-count":shard_count} 
        return JSONResponse(message,status_code=200,media_type='application/json')       


@app.route('/id-correct/{id}')
class Correct(HTTPEndpoint):
    def put(self, request):
        global native_shard_id
        native = request.path_params['id']
        native_shard_id = native
        logging.debug("Our new native id: %s",native_shard_id)
        message = {"message": "here is your shard-id.",
                   "shard-id": new_native}
        return JSONResponse(message, status_code=200, media_type='application/json')


@app.route('/member-list/{id}')
class MemList(HTTPEndpoint):
    async def put(self, request):
        data = await request.json()
        theShard = request.path_params['id']
        newAddress = data['socket-address']
        groupList[int(theShard)].addGroupMember(newAddress)
        message = {"message": "Added new member."}
        return JSONResponse(message, status_code=200, media_type='application/json')


@app.route('/key-value-store-shard/reshard')
class Reshard(HTTPEndpoint):
    async def put(self, request):
        global shardIDs
        global idList
        global groupList
        logging.debug("Reshard has been requested.")
        data = await request.json()
        requestedShardCount = data['shard-count']
        newIndex = len(groupList)
        numReplicas = len(view.array())
        logging.debug("Number of replicas: %s", numReplicas)
        logging.debug("Request shard count: %s", requestedShardCount)
        if (2*requestedShardCount) > numReplicas:
                message = {
                    "message": "Not enough nodes to provide fault-tolerance with the given shard count!"}
                return JSONResponse(message, status_code=400, media_type='application/json')
        else:
            # add a new ReplicaGroup to groupList
            for group in groupList:
                logging.debug("Group %s:%s before rotation", group.getShardID(),
                              group.getMembers())
            group = "group" + str(newIndex)
            tempGroupID = zlib.crc32(group.encode('utf-8'), 0)
            logging.debug(group + " hashed to "+str(tempGroupID))
            newShard = shard.ReplicaGroup(newIndex, tempGroupID, 0, [], 0, {})
            groupList.append(newShard)
            logging.debug("Added new shard to groupList.")

            idList.append(str(newIndex))
            shardIDs = ",".join(idList)

            # tell other replicas in our VIEW to add the new Replica Group to their GroupList
            logging.debug("Added index %s to id list.", newIndex)
            for i, v in enumerate(view):
                logging.debug(
                    "Broadcasting add-shard to %s%s/add-shard/%s", BASE, view[i], newIndex)
            rs = (grequests.put(BASE + str(address) + '/add-shard/'+str(newIndex),
                                )for address in view)
            grequests.map(rs)

            # We need to do the balancing here
            for i, group in enumerate(groupList):
                if i != newIndex and len(group.getMembers()) > 2:
                    logging.debug("Group %s before removal: %s",
                                  group.getShardID(), group.getMembers())
                    tempIP = group.shard_id_members.pop(0)
                    if tempIP == OWN_SOCKET:
                        native_shard_id = newIndex
                    logging.debug("%s removed from group %s",
                                  tempIP, group.getShardID())
                    groupList[newIndex].addGroupMember(tempIP)
                    logging.debug("Added %s to group %s.", tempIP,
                                  groupList[newIndex].getShardID())
            for group in groupList:
                logging.debug("Group %s after rotation:%s",
                              group.getShardID(), group.getMembers())
            if native_shard_id == len(groupList):
                kvsaddr = groupList[native_shard_id + 1].shard_id_members[0]
            else:
                kvsaddr = groupList[0].shard_id_members[0]
            kvsmsg = {'shard-hash': groupList[native_shard_id].getHashID()}
            kvsreq = grequests.get(BASE + str(kvsaddr)  + '/set-key/', json = kvsmsg)
            temp = grequests.map([kvsreq])
            newKVSdata = temp[0].json()
            kvstorage.kvs = jsonpickle.decode(newKVSdata['kvs'])
            logging.debug("kvstorage: %s", kvstorage.kvs)
            groupList[native_shard_id].key_count = len(kvstorage.kvs)
            logging.debug("CURRENT key count for us: %s", groupList[native_shard_id].key_count)
            logging.debug("our key count:%s",groupList[native_shard_id].key_count)
            await forwarding(None, None, True, reqType="HISTORY")


            message = {
                "message": "Resharding done successfully"}
            return JSONResponse(message, status_code=200, media_type='application/json')


@app.route('/add-shard/{index}')
class Build(HTTPEndpoint):
    async def put(self, request):
        global groupList
        global shardIDs
        global idList
        logging.debug("Inside add-shard at: %s", OWN_SOCKET)
        index = request.path_params['index']
        logging.debug("Adding shard %s", index)
        #newAddress = data['socket-address']
        #groupList[int(theShard)].addGroupMember(newAddress)
        group = "group" + str(index)
        tempGroupID = zlib.crc32(group.encode('utf-8'), 0)
        logging.debug(group+" hashed to "+str(tempGroupID))
        newShard = shard.ReplicaGroup(index, tempGroupID, 0, [], 0, {})
        groupList.append(newShard)
        idList.append(newShard.getShardID())
        shardIDs = ",".join(idList)

        for i, group in enumerate(groupList):
           if i != index and len(group.getMembers()) > 2:
                logging.debug("Group %s before removal: %s",
                              group.getShardID(), group.getMembers())
                tempIP = group.shard_id_members.pop(0)
                if tempIP == OWN_SOCKET:
                    native_shard_id = index
                groupList[int(index)].addGroupMember(tempIP)
                logging.debug("Added %s to group %s.", tempIP,
                              groupList[int(index)].getShardID())

        for group in groupList:
                logging.debug("Group %s after rotation:%s",
                              group.getShardID(), group.getMembers())

        message = {"message": "Added new member."}
        return JSONResponse(message, status_code=200, media_type='application/json')

@app.route('/set-key/')
class KeySet(HTTPEndpoint):
    async def get(self, request):
        data = await request.json()
        shardHash = data['shard-hash']

        delKeys = {}
        for k in kvstorage.kvs:
            key = zlib.crc32(k.encode('utf-8'))
            if key < shardHash:
                delKeys[k] = kvstorage.kvs[k]
        # delete keys
        all(map(kvstorage.kvs.pop, delKeys))

        groupList[native_shard_id].key_count = len(kvstorage.kvs)
        await forwarding(None, None, True, reqType="HISTORY")
        # delete keys from replicas
        alist = groupList[native_shard_id].getMembers().copy()
        if OWN_SOCKET in alist:
            alist.remove(OWN_SOCKET)
        if  (OWN_SOCKET == groupList[native_shard_id].getMembers()[0]):
            kvsreq = (grequests.get(BASE + address  + '/set-key/', json = data) for address in alist )
            grequests.map(kvsreq)

        message = {'kvs': jsonpickle.encode(delKeys)}
        return JSONResponse(message, status_code=200, media_type='application/json')



def forwardToShard(shardID, key, data, requestType):
        logging.debug("Forwarding request to: Shard-ID: %s Key: %s ReqType: %s",
                      shardID, key, requestType)
        # TODO: CHange this line for proper sharting:
        addr = groupList[int(shardID)].getMembers()[0] 
        # this one ^

        destinationAddress = BASE + addr + KVS_ENDPOINT + key
        logging.debug("forwarding destination: " +destinationAddress)
        if requestType == "PUT": 
            rs =  grequests.put(destinationAddress, json =  data)
        elif requestType == "DELETE":
            rs =  grequests.delete(destinationAddress,json =  data)
        elif requestType == "GET":
            rs =  grequests.get(destinationAddress,json = data)
        else:
            logging.error("Oops I sharded! Changing pants\nforwarding reqType invalid!!!")

        resp = grequests.map([rs])
        return resp[0]


def exception_handler(request, exception):
    logging.warning("Replica may be down! Timeout on address: %s", request.url)
    deleteReplica(request.url)

def retrieveStore():
    # The opposite of GET /store/
    # Asks another replica for the store,
    # then replaces this store with the retreived data
    global view
    global shardIDs
    global shard_count
    global groupList
    

    logging.warning("Running Retrieve Store")
    try:
        newStore =  grequests.get(BASE + view[attempt] + "/store/", timeout=TIMEOUT_TIME)
        if not all (k in newStore for k in ('kvs', 'history', 'view', 'shard-ids',
                                        'shard-count', 'pending')):
            logging.error("The store endpoint did not work as intended")
        kvstorage.kvs       = jsonpickle.decode(newStore['kvs'])
        kvstorage.history   = jsonpickle.decode(newStore['history'])
        view                = jsonpickle.decode(newStore['view'])
        shardIDs            = jsonpickle.decode(newStore['shard-ids'])
        shard_count         = jsonpickle.decode(newStore['shard-count'])
        groupList           = jsonpickle.decode(newStore['group-list'])
        for p in jsonpickle.decode(newStore['shard-count']):
            kvstorage.dataMgmt(p[0],p[1])
    except:
        logging.warning("Timeout occured while running retreive store!!")

def deleteReplica(ip):
    # Manage view and groupList locally
    # broacast that change
    # rebalance if needed 
    global view
    logging.warning("%s is down, adjusting view and grouplist", ip)
    view.remove(ip)
    rg = getRepGroup(ip)
    if rg:
        rg.shard_id_members.remove(ip)
    else:
        logging.error("deleteReplica could not find given ip: %s in replica group", ip)
    
    forwarding(None, ip, True, "VIEW_DELETE")

    if len(rg.shard_id_members) < 2:
        logging.warning("balance needed!")
        balance(groupList.index(rg), groupList)

def getRepGroup(ip):
    for i in groupList:
        if ip in i.shard_id_members:
            return i
    return False


                
# TODO: views startup
@app.on_event('startup')
async def startup():
    global view

    retrieveStore()


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080, lifespan = "auto" )

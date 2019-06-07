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
shard_count = os.environ['SHARD_COUNT']
groupList = []
TIMEOUT_TIME = 3

# Process-specific constants to help set up and record itself within it's assigned shard.
shardIDs = ""
view = views.ViewList(os.environ['VIEW'], OWN_SOCKET)
groupList = []
idList = []
ip = zlib.crc32(OWN_SOCKET.encode('utf-8'))
native_shard_id = 0

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
logging.debug("Chord is being initialized. Shard count: %s", shard_count)
# Statically handle the first two nodes we'll always need.
for i in range(int(shard_count)):
    group = "group" + str(i)
    tempGroupID = zlib.crc32(group.encode('utf-8'),0)
    logging.debug("Group "+str(i)+" hashed to "+str(tempGroupID))
    tempGroup = shard.ReplicaGroup(i,tempGroupID,0,[],0,{})
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

groupList.sort(key=lambda x: x.hash_id, reverse=False)

    

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
        data = await request.json()
        
        key = request.path_params['key']
        value = ""
        version = ""
        causalMetadata = []
        reqType = request
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

        # the key in the request didn't hash into our group id, we have to redirect 
        # it to the proper replica group rather than process it ourselves.
        # we also are going to be responsible for sending the repsone back 
        # to the client.
        for i in range(0,len(groupList)-1):
            logging.debug("i = %s gList[i].hash_id: %s, gList[i+1].hash_id:%s",i,groupList[i].getHashID(),groupList[i+1].getHashID())
            if keyCrc > groupList[i].hash_id and keyCrc < groupList[i+1].hash_id:
                logging.debug("found spot for key between %s and %s",i,i+1)
                if native_shard_id != groupList[i+1].getShardID():
                    #forward to other shard group
                    return forwardToShard(groupList[i+1].getShardID(),key,data,"PUT")
                    # the key hashed out to the proper group id, process and forward like usual.
                else:
                    logging.debug("key is in correct shard. doing normal operations.")
                    if 'version' in data:  # version
                        version = data['version']
                    else:
                        version = random.randint(0, 1000)
                        logging.info("No Version in data, generating a unique version id: %s", version)

                    senderSocket = request.client.host + ":8080"
                    logging.debug("senderSocket check: %s in view: %s, %s",
                                senderSocket, view, senderSocket in view)

                    logging.debug("===Put at %s : %s", key, value)

                    # Second, we set up the task that will update the value,
                    # add the request to the store incase a copy is made while pending,
                    # and, set up the forwardinging that will run in the background after
                    # the request is completed
                    # https://www.starlette.io/background/
                    vs = kvstorage.ValueStore(value, version, causalMetadata.copy())
                    req = (key, vs)
                    pendingRequests.append(req)

                    isUpdating = await kvstorage.dataMgmt(key, vs)
                    pendingRequests.remove()
                    task = BackgroundTask(
                        forwarding, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="PUT")

                    pendingRequests.remove(req)


                    # Finally we return
                    causalMetadata.append(version)
                    if isUpdating:
                        message = {
                            "message": "Updated successfully",
                            "version": version,
                            "causal-metadata": causalMetadata,
                            "shard-id": str(native_shard_id)
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
                            "shard-id": str(native_shard_id)
                        }
                        return JSONResponse(message,
                                            status_code=201,
                                            background=task,
                                            media_type='application/json')
            elif i == len(groupList)-1 :
                if groupList[i+1].getHashID() < keyCrc :
                    return forwardToShard(groupList[i+1].getShardID(),key,data,"PUT")


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
        keyCrc = zlib.crc32(key.encode('utf-8'))
        logging.debug(key + " hashed to " + str(keyCrc))

        version = ""
        causalMetadata = []

        if len(key) > 50:  # key
            message = {"error": "Key is too long",
                       "message": "Error in DELETE"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'version' in data:  # version
            version = data['version']
        else:
            version = random.randint(0, 1000)
            logging.info(
                "No Version in data, generating a unique version id: %s", version)
        if 'causal-metadata' in data:  # causalMetadata
            causalMetadata = data['causal-metadata']
            if causalMetadata == "":  # Case empty string is passed
                causalMetadata = []
        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s",
                      senderSocket, view, senderSocket in view)

        logging.debug("===Delete at %s", key)
                # the key in the request didn't hash into our group id, we have to redirect 
        # it to the proper replica group rather than process it ourselves.
        # we also are going to be responsible for sending the repsone back 
        # to the client.
        if(groupID != procNodeID):
            logging.debug("this key is not in our shard.")
            return forwardToShard(groupID, key, data, "DELETE")
        # Second, we set up the task that will update the value,
        # and the forwarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = kvstorage.ValueStore(None, version, causalMetadata.copy())
        req = (key, vs)
        pendingRequests.append(req)
        isDeleting = await kvstorage.dataMgmt(key, vs)
        task = BackgroundTask(
            forwarding, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="DELETE")
        pendingRequests.remove(req)

        # Finally we return
        causalMetadata.append(version)
        if isDeleting:
            shardDeleteID = groupList[procNodeID-1].getHashID()
            message = {
                "message": "Deleted successfully",
                "version": version,
                "causal-metadata": causalMetadata,
                "shard-id": shardDeleteID
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
        logging.debug(key + " hashed to " + keyCrc.hexdigest())
        if(groupID != procNodeID):
            logging.debug("this key is not in our shard.")
            return forwardToShard(groupID, key, data, "GET")
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
        "view": jsonpickle.encode(view),
        "shard-ids": jsonpickle.encode(groupList),
        "shard-count": jsonpickle.encode(shard_count),
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
def getNodeID(self):
    logging.debug("Shard ID to be returned: %s",native_shard_id)
    message = {
        "message": "Shard ID of the node retrieved successfully", 
        "shard-id": native_shard_id}
    return JSONResponse(message,status_code=200,media_type='application/json')
    

@app.route('/key-value-store-shard/shard-id-members/{shard}')
class Members(HTTPEndpoint):
    async def get(self,request):
        shard = request.path_params['shard']
        logging.debug("ID requested: %s", shard)
        group = groupList[int(shard)-1]
        groupString = group.getReplicas()
        message = {"message": "Members of shard ID retrieved successfully", 
        "shard-id-members":groupString}
        return JSONResponse(message,status_code=200,media_type='application/json')


@app.route('/key-value-store-shard/shard-id-key-count/{id}')
def getNumKeys(request):
    message = {"message": "Key count of shard ID retrieved successfully",
               "shard-id-key-count": groupList[request.path_params['id']].getReplicas()}
    return JSONResponse(message,status_code=200,media_type='application/json')


@app.route('/key-value-store-shard/add-member/{newReplica}')
class AddReplica(HTTPEndpoint):
    async def put(self,request):
        senderSocket = request.client.host + ":8080"
        data = await request.json()
        big = 1
        dest = 0
        if 'socket-address' in data:
            newAddress = data['socket-address']
        else:
            message = {"error": "No address given.",
                       "message": "Error in DELETE"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        newHashedAddress = zlib.crc32(newAddress.encode('utf-8'))
        
        for i in range(len(groupList)):
            if newHashedAddress < groupList[i].getHashID():
                groupList[i].addGroupMember(newAddress)
                dest = i
                big = 0
        if big == 1:
            groupList[0].addGroupMember(newAddress)
        forwarding(None,vs=newAddress, isFromClient = senderSocket not in view, reqType = "PUT")
        view.add(newAddress)
        logging.debug("New members list for replica group " + groupList[dest].getNodeID() + ": "+ groupList[dest].getReplicas())
        logging.debug(view)

            



def forwardToShard(shardID, key, data, requestType):
        logging.debug("Forwarding request to: Shard-ID: %s Key: %s ReqType: %s",
                      shardID, key, requestType)
        # TODO: CHange this line for proper sharting:
        addr = groupList[shardID].getMembers()[0] 
        # this one ^

        destinationAddress = BASE + addr + KVS_ENDPOINT + key
        logging.debug(destinationAddress)
        if requestType == "PUT": 
            return grequests.put(destinationAddress, data)
        elif requestType == "DELETE":
            return grequests.delete(destinationAddress, data)
        elif requestType == "GET":
            return grequests.get(destinationAddress, data)
        else:
            logging.error("Oops I sharded! Changing pants\nforwarding reqType invalid!!!")

async def forwarding(key, vs, isFromClient, reqType):
    if isFromClient:
        logging.debug("putforwarding at: Key: %s ReqType: %s View: %s",
                      key, reqType, groupList[native_shard_id].getMembers())
        logging.debug(BASE + address + KVS_ENDPOINT + key)
        if reqType == "PUT":
            rs = (grequests.put(BASE + address + KVS_ENDPOINT + key,
                                json={'value': vs.getValue(),
                                      'version': vs.getVersion(),
                                      'causal-metadata': vs.causalMetadata}) for address in groupList[native_shard_id].getMembers())
        elif reqType == "DELETE":
            rs = (grequests.delete(BASE + address + KVS_ENDPOINT + key,
                                   json={'version': vs.getVersion(),
                                         'causal-metadata': vs.causalMetadata}) for address in groupList[native_shard_id].getMembers())
        elif reqType == "VIEW_DELETE":
            rs = (grequests.delete(BASE + address + VIEW_ENDPOINT,
                                   json={'socket-address': vs}) for address in groupList[native_shard_id].getMembers())
        else:
            logging.error("forwarding reqType invalid!!!")
        grequests.map(rs, exception_handler=exception_handler,
                      gtimeout=TIMEOUT_TIME)
        logging.debug("putforwarding Finished")
    else:
        logging.debug(
            "request is from a replica (not a client), not forwardinging")


def exception_handler(request, exception):
    logging.warning("Replica may be down! Timeout on address: %s", request.url)
    repairView(request.url)

def retrieveStoreHelper():
    ging.warning("This could be because this process is the first one booted, or something worse happened") 

def retrieveStore():
    # The opposite of GET /store/
    # Asks another replica for the store,
    # then replaces this store with the retreived data
    global view
    global shardIDs
    global shard_count

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
        for p in jsonpickle.decode(newStore['shard-count']):
            kvstorage.dataMgmt(p[0],p[1])

    except:
        logging.warning("Timeout occured while running retreive store!!")

    





def repairView(downSocket):
    global view
    logging.warning("%s is down, adjusting view", downSocket)
    view.remove(downSocket)
    forwarding(None, downSocket, True, "VIEW_DELETE")

                
# TODO: views startup
@app.on_event('startup')
async def startup():
    global view

    retrieveStore()


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080, lifespan = "auto" )

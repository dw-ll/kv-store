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
import math

# Setup
logging.basicConfig(level=logging.DEBUG)
app = Starlette(debug=True)
keySha = hashlib.sha1()
procSha = hashlib.sha1()

# Constants
BASE = 'http://'
KVS_ENDPOINT = '/key-value-store/'
VIEW_ENDPOINT = '/key-value-store-view/'
SHARD_ENDPOINT = '/key-value-store-shard/'
OWN_SOCKET = os.environ['SOCKET_ADDRESS']
shard_count = os.environ['SHARD_COUNT']
# List of ReplicaGroup objects


def balance(index, fullList):
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

shardIDs = "1,2"
# Hashing for this specific process.
procSha.update(OWN_SOCKET.encode('utf-8'))
procNodeID = (int(procSha.hexdigest(),16) % 2) + 1
TIMEOUT_TIME = 3
view = views.ViewList(os.environ['VIEW'], OWN_SOCKET)
groupList = []
ipSHA = hashlib.sha1()
logging.debug("Chord is being initialized. Shard count: %s", shard_count)
# Statically handle the first two nodes we'll always need.
r1 = shard.ReplicaGroup(1,shard_count,[],0,{})
r2 = shard.ReplicaGroup(2, shard_count, [], 0,{})
groupList.append(r1)
groupList.append(r2)
addr = os.environ['SOCKET_ADDRESS']
logging.debug("addr is " + addr)
ipSHA.update(addr.encode('utf-8'))
logging.debug("identifier is " + str(ipSHA.hexdigest()))
logging.debug("length of groupList: %s",len(groupList))
#logging.debug("Identifier for "+OWN_SOCKET + "= " + str(ipSHA.hexdigest()))
hashedGroupID = (int(ipSHA.hexdigest(), 16) % 2) + 1
#logging.debug(OWN_SOCKET + " will be in replica group: " + str(hashedGroupID))
logging.debug(addr + " is going to group " + str(hashedGroupID))
groupList[hashedGroupID-1].incrementKeyCount()
groupList[hashedGroupID-1].addGroupMember(OWN_SOCKET)
logging.debug(view)
for other in view:
    hasher = hashlib.sha1()
    hasher.update(other.encode('utf-8'))
    logging.debug("Identifier for "+other +
                    " = " + str(hasher.hexdigest()))
    hasherGroup = (int(hasher.hexdigest(), 16) % 2) + 1
    logging.debug(other + " is going to group "+str(hasherGroup))
    if hasherGroup == 1:
        groupList[0].addGroupMember(other)
    else:
        groupList[1].addGroupMember(other)


list1 = groupList[0].shard_id_members
list2 = groupList[1].shard_id_members

# M replicas, N replica groups
# M = 7, N = 2, M > 2(N) => M > 4 : TRUE
# M = 7, N = 10, M > 2(N) => M > 20 : TRUE, M = 6 so FALSE
# 
# if condition met, start for loop 
if len(list1) < 2: 
    # some balancing code.
    balance(0,groupList)
elif len(list2) < 2:
    balance(1,groupList)

logging.debug(groupList)

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

        data = await request.json()
        
        key = request.path_params['key']
        value = ""
        version = ""
        causalMetadata = []
        reqType = request
        keySha.update(key.encode('utf-8'))
        logging.debug(key + " hashed to " + keySha.hexdigest())
        logging.debug("Looking up replica group for " + keySha.hexdigest())
        groupID = (int(keySha.hexdigest(), 16)) % 2 + 1
        logging.debug(keySha.hexdigest() + "to be put at group:" + str(groupID))
        putShardID = groupList[procNodeID-1].getReplicaGroupID()

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
        if(groupID != procNodeID):
            logging.debug("this key is not in our shard.")

            return forwardToShard(groupID, key, data, "PUT")


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
            # and the forwardinging that will run in the background after
            # the request is completed
            # https://www.starlette.io/background/
            vs = kvstorage.ValueStore(value, version, causalMetadata.copy())
            isUpdating = await kvstorage.dataMgmt(key, vs)
            task = BackgroundTask(
                forwarding, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="PUT")

            # Finally we return
            causalMetadata.append(version)
            if isUpdating:
                message = {
                    "message": "Updated successfully",
                    "version": version,
                    "causal-metadata": causalMetadata,
                    "shard-id": putShardID
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
                    "shard-id": putShardID
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
        keySha.update(key.encode('utf-8'))
        logging.debug(key + " hashed to " + keySha.hexdigest())

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
        isDeleting = await kvstorage.dataMgmt(key, vs)
        task = BackgroundTask(
            forwarding, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="DELETE")

        # Finally we return
        causalMetadata.append(version)
        if isDeleting:
            shardDeleteID = groupList[procNodeID-1].getReplicaGroupID()
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
        keySha.update(key.encode('utf-8'))
        logging.debug(key + " hashed to " + keySha.hexdigest())
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
        "kvs": json.dumps(kvstorage.kvs, cls=kvstorage.kvsEncoder),
        "history": kvstorage.history,
        "view": tView
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
    group = groupList[procNodeID-1]
    groupID = group.getReplicaGroupID()
    logging.debug("Shard ID to be returned: %s",groupID)
    message = {
        "message": "Shard ID of the node retrieved successfully", 
        "shard-id": groupID}
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
               "shard-id-key-count": groupList[request.path_params['id']-1].getReplicas()}
    return JSONResponse(message,status_code=200,media_type='application/json')


def forwardToShard(shardID, key, data, requestType):
        logging.debug("Forwarding request to: Shard-ID: %s Key: %s ReqType: %s",
                      shardID, key, requestType)
        # TODO: CHange this line for proper sharting:
        addr = groupList[shardID-1].getReplicas().split(',')[0] 
        # this one ^

        destinationAddress = BASE + addr + KVS_ENDPOINT + key, data
        if requestType == "PUT": 
            return grequests.get(destinationAddress, data)
        elif requestType == "DELETE":
            return grequests.delete(destinationAddress, data)
        elif requestType == "GET":
            return grequests.delete(destinationAddress, data)
        else:
            logging.error("Oops! Your code sharted all over everything!\nforwarding reqType invalid!!!")

async def forwarding(key, vs, isFromClient, reqType):
    if isFromClient:
        logging.debug("putforwarding at: Key: %s ReqType: %s View: %s",
                      key, reqType, view)
        if reqType == "PUT":
            rs = (grequests.put(BASE + address + KVS_ENDPOINT + key,
                                json={'value': vs.getValue(),
                                      'version': vs.getVersion(),
                                      'causal-metadata': vs.causalMetadata}) for address in view)
        elif reqType == "DELETE":
            rs = (grequests.delete(BASE + address + KVS_ENDPOINT + key,
                                   json={'version': vs.getVersion(),
                                         'causal-metadata': vs.causalMetadata}) for address in view)
        elif reqType == "VIEW_DELETE":
            rs = (grequests.delete(BASE + address + VIEW_ENDPOINT,
                                   json={'socket-address': vs}) for address in view)
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

def retrieveStore():
    logging.warning("Running Retrieve Store")


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

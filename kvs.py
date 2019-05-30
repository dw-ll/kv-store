from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.background import BackgroundTask
from starlette.endpoints import HTTPEndpoint
import uvicorn
import grequests

import logging
import asyncio
import random
import os


class ValueStore:
    def __init__(self, value, version, cm):
        self.versions = []
        self.values = []
        self.causalMetadata = cm
        self.update(value, version)

    def getValue(self, index=-1):
        return self.values[index]

    def getVersion(self, index=-1):
        return self.versions[index]

    def update(self, value, version):
        self.values.append(value)
        self.versions.append(version)

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class ViewList:
    def __init__(self, startString, ownSocket):
        s = startString.split(',')
        for a in s:
            if a.split(":")[0] == ownSocket.split(":")[0]:
                s.remove(a)
                self.ownSocket = a
                break
        self.viewArray = s

    def array(self):
        return self.viewArray

    def remove(self, delAddress):
        self.viewArray.remove(delAddress)

    def add(self, addAddress):
        self.viewArray.append(addAddress)

    def __repr__(self):
        x = ","
        x = x.join(self.viewArray)
        return x

    def __getitem__(self, key):
        return self.viewArray[key]


# Setup
logging.basicConfig(level=logging.DEBUG)
app = Starlette(debug=True)


# Constants
BASE = 'http://'
KVS_ENDPOINT = '/key-value-store/'
VIEW_ENDPOINT = '/key-value-store-view/'
OWN_SOCKET = os.environ['SOCKET_ADDRESS']
TIMEOUT_TIME = 3

# Globals
kvs = {}
history = []
view = ViewList(os.environ['VIEW'], OWN_SOCKET)



# TODO:  delete, views

@app.route('/key-value-store/{key}')
class KeyValueStore(HTTPEndpoint):
    # Put handler for KeyValueStore endpoint
    #   Loads data from reuqest
    #   Sets up background tasks to
    #       Store value in key
    #       TODO: Foward changes if message is from a client
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

        if len(key) > 50:  # key
            message = {"error": "Key is too long",
                       "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'value' in data:  # value
            value = data['value']
        else:
            message = {"error": "Value is missing", "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'version' in data:  # version
            version = data['version']
        else:
            version = random.randint(0, 1000)
            logging.info(
                "No Version in data, generating a unique version id: %s", version)
        if 'causal-metadata' in data:  # causalMetadata
            causalMetadata = data['causal-metadata']
        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s", senderSocket, view, senderSocket in view)

        logging.debug("===Put at %s : %s", key, value)

        # Second, we set up the task that will update the value,
        # and the fowarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = ValueStore(value, version, causalMetadata.copy())
        isUpdating = await putData(key, vs)
        task = BackgroundTask(putFoward, key=key, vs=vs, isFromClient=senderSocket not in view)

        # Finally we return
        causalMetadata.append(version)
        if isUpdating:
            message = {
                "message": "Updated successfully",
                "version": version,
                "causal-metadata": causalMetadata
            }
            return JSONResponse(message,
                                status_code=200,
                                background=task,
                                media_type='application/json')
        else:
            message = {
                "message": "Added successfully",
                "version": version,
                "causal-metadata": causalMetadata
            }
            return JSONResponse(message,
                                status_code=201,
                                background=task,
                                media_type='application/json')
    
    # Get Handler for key value store endpoint
    #   returns to the client with the current state of the key
    async def get(self, request):
        key = request.path_params['key']
        if key in kvs:
            vs = kvs[key]
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

# The async part of the keyvaluestore put
#   checks if its allowed to run yet,
#       if not it pauses and allows
#       other code to run
# Returns True if put is an update
#         False if put is an add
async def putData(key, vs):
    value = vs.getValue()
    version = vs.getVersion()
    causalMetadata = vs.causalMetadata
    logging.debug("+++PutData started at:\n   Key: %s\n   Value: %s\n   CM:%s",
                  key, value, causalMetadata)
    while(not isInCausalOrder(causalMetadata)):
        logging.debug("%s not in %s, waiting request", causalMetadata, history)
        await asyncio.sleep(5)
    history.append(version)
    logging.info("Added %s to history", version)
    logging.debug("New History: %s", history)
                 
    if key in kvs:
        kvs[key].update(value, version)
        logging.info("---PutData finished with kvs UPDATED: %s => %s",
            key, kvs[key].getValue())
        return True
    else:
        kvs[key] = vs
        logging.info("---PutData finished with kvs ADDED: %s => %s",
            key, kvs[key].getValue())
        return False


# Checks if the causalMetadata is
# running in causal order
def isInCausalOrder(causalMetadata):
    if(causalMetadata):
        logging.debug("Request *is* causally dependent on: %s", causalMetadata)
        for v in causalMetadata:
            if v not in history:
                logging.warning("Request *is* not in causal order!")
                return False
    return True

# if request should be fowarded,
# fowards put at key, vs to all
# replicas in view
async def putFoward(key, vs, isFromClient):
    if isFromClient:
        value = vs.getValue()
        version = vs.getVersion()
        causalMetadata = vs.causalMetadata
        logging.debug("putFoward at: Key: %s Value: %s View: %s", key, value, view)

        rs = (grequests.put(BASE + address + KVS_ENDPOINT + key,
                            json={'value': value,
                                  'version': version,
                                  'causal-metadata': causalMetadata}) for address in view)
        grequests.map(rs, exception_handler=exception_handler)
        logging.debug("putFoward Finished")
    else:
        logging.debug("request is from a replica (not a client), not fowarding")
          
def exception_handler(request, exception):
    logging.warning("Replica may be down! Timeout on address: %s", request.url)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)

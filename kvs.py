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
import json


class ValueStore:
    def __init__(self, value, version, cm):
        self.versions = []
        self.values = []
        self.causalMetadata = cm
        self.values.append(value)
        self.versions.append(version)

    def getValue(self, index=-1):
        return self.values[index]

    def getVersion(self, index=-1):
        return self.versions[index]

    def update(self, vs):
        self.values.append(vs.getValue())
        self.versions.append(vs.getVersion())
        self.causalMetadata.append(vs.causalMetadata)

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
        # return dict(
        #     versions=self.versions,
        #     values=self.values,
        #     causalMetadata=self.causalMetadata
        #     )
        #return json.dumps(self, default=lambda o: o.__dict__,
        #                  sort_keys=True, indent=4)
class kvsEncoder(json.JSONEncoder):
    def default(self,obj):
        if isinstance(obj, ValueStore):
            return {"values":obj.values,
                    "versions" : obj.versions,
                    "causalMetadata" : obj.causalMetadata}
        return kvsEncoder.default(self,obj)

    def generate(self):
        # self.casualMetadata.append(self.version)
        self.version = self.version + 1



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
        if x:
            x = self.ownSocket + "," + x
        else:
            x = self.ownSocket

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



# TODO: views startup

@app.route('/key-value-store/{key}')
class KeyValueStore(HTTPEndpoint):
    # Put handler for KeyValueStore endpoint
    #   Loads data from reuqest
    #   Sets tasks to
    #        Store value in key - (Await)
    #        Foward changes if message is from a client - (Background)
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
            if causalMetadata == "": # Case empty string is passed
                causalMetadata = []
        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s", senderSocket, view, senderSocket in view)

        logging.debug("===Put at %s : %s", key, value)

        # Second, we set up the task that will update the value,
        # and the fowarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = ValueStore(value, version, causalMetadata.copy())
        isUpdating = await dataMgmt(key, vs)
        task = BackgroundTask(foward, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="PUT")

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
            if causalMetadata == "": # Case empty string is passed
                causalMetadata = []
        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s", senderSocket, view, senderSocket in view)

        logging.debug("===Delete at %s", key)

        # Second, we set up the task that will update the value,
        # and the fowarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = ValueStore(None, version, causalMetadata.copy())
        isDeleting = await dataMgmt(key, vs)
        task = BackgroundTask(foward, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="DELETE")

        # Finally we return
        causalMetadata.append(version)
        if isDeleting:
            message = {
                "message": "Deleted successfully",
                "version": version,
                "causal-metadata": acausalMetadata
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

@app.route('/key-value-store-view/')
class KVSView(HTTPEndpoint):
    async def get(self, request):
        message = {"message": "View retreived successfully",
                    "view": repr(view)}
        return JSONResponse(message, status_code=200, media_type='application/json')
    async def put(self, request):
        global view
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
        global view

        data = await request.json()
        if 'socket-address' in data:
            delAddress = data['socket-address']
        else:
            # Case socket address is not in data
            message = {"error": "Value is missing", "message": "Error in DELETE"}
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
        "kvs": json.dumps(kvs, cls=kvsEncoder),
        "history": history,
        "view": tView
    }
    return JSONResponse(message, status_code=200, media_type='application/json')
    # TODO: Check for pending requests
     
 



# The async part of the keyvaluestore put
#   checks if its allowed to run yet,
#       if not it pauses and allows
#       other code to run
# Returns True if put is an update
#         False if put is an add
async def dataMgmt(key, vs):
    global history
    value = vs.getValue()
    version = vs.getVersion()
    causalMetadata = vs.causalMetadata
    logging.debug("+++dataMgmt started at:\n   Key: %s\n   Value: %s\n   CM:%s",
                  key, value, causalMetadata)
    while(not isInCausalOrder(causalMetadata)):
        logging.debug("%s not in %s, waiting request", causalMetadata, history)
        await asyncio.sleep(5)
    history.append(version)
    logging.info("Added %s to history", version)
    logging.debug("New History: %s", history)

    vs.causalMetadata.append(version)             
    if key in kvs:
        kvs[key].update(vs)
        logging.info("---PutData finished with True: %s => %s",
            key, kvs[key].getValue())
        return True
    else:
        if value:
            kvs[key] = vs
        logging.info("---dataMgmt finished with False: %s => %s",
                key, value) 
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
# fowards PUT at (key, vs) to all
# replicas in view

async def foward(key, vs, isFromClient, reqType):
    if isFromClient:
        logging.debug("putFoward at: Key: %s ReqType: %s View: %s", key, reqType, view)
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
            logging.error("foward reqType invalid!!!")
        grequests.map(rs, exception_handler=exception_handler, gtimeout=TIMEOUT_TIME)
        logging.debug("putFoward Finished")
    else:
        logging.debug("request is from a replica (not a client), not fowarding")
          
def exception_handler(request, exception):
    logging.warning("Replica may be down! Timeout on address: %s", request.url)
    repairView(request.url)

def repairView(downSocket):
    global view
    logging.warning("%s is down, adjusting view", downSocket)
    view.remove(downSocket)
    foward(None, downSocket, True, "VIEW_DELETE")

def retrieveStore():
    logging.warning("Running Retrieve Store")

@app.on_event('startup')
async def startup():
    retrieveStore()

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)

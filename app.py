from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.background import BackgroundTask
from starlette.endpoints import HTTPEndpoint
import uvicorn
import grequests
import logging
import os
import json
import random
import kvstorage
import views
import hashlib

# Setup
logging.basicConfig(level=logging.DEBUG)
app = Starlette(debug=True)
sha = hashlib.sha256()


# Constants
BASE = 'http://'
KVS_ENDPOINT = '/key-value-store/'
VIEW_ENDPOINT = '/key-value-store-view/'
OWN_SOCKET = os.environ['SOCKET_ADDRESS']
TIMEOUT_TIME = 3
view = views.ViewList(os.environ['VIEW'], OWN_SOCKET)

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
        hashedKey = sha.update(key.encode('utf-8'))
        logging.debug(key + " hashed to "+ sha.hexdigest())


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
            if causalMetadata == "":  # Case empty string is passed
                causalMetadata = []
        senderSocket = request.client.host + ":8080"
        logging.debug("senderSocket check: %s in view: %s, %s",
                      senderSocket, view, senderSocket in view)

        logging.debug("===Put at %s : %s", key, value)

        # Second, we set up the task that will update the value,
        # and the fowarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = kvstorage.ValueStore(value, version, causalMetadata.copy())
        isUpdating = await kvstorage.dataMgmt(key, vs)
        task = BackgroundTask(
            foward, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="PUT")

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
        hashedKey = sha.update(key.encode('utf-8'))
        logging.debug(key + " hashed to " + sha.hexdigest())

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

        # Second, we set up the task that will update the value,
        # and the fowarding that will run in the background after
        # the request is completed
        # https://www.starlette.io/background/
        vs = kvstorage.ValueStore(None, version, causalMetadata.copy())
        isDeleting = await kvstorage.dataMgmt(key, vs)
        task = BackgroundTask(
            foward, key=key, vs=vs, isFromClient=senderSocket not in view, reqType="DELETE")

        # Finally we return
        causalMetadata.append(version)
        if isDeleting:
            message = {
                "message": "Deleted successfully",
                "version": version,
                "causal-metadata": causalMetadata
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
        hashedKey = sha.update(key.encode('utf-8'))
        logging.debug(key + " hashed to " + sha.hexdigest())
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
        views.view
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
        views.view

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

# if request should be fowarded,
# fowards PUT at (key, vs) to all
# replicas in view


async def foward(key, vs, isFromClient, reqType):
    if isFromClient:
        logging.debug("putFoward at: Key: %s ReqType: %s View: %s",
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
            logging.error("foward reqType invalid!!!")
        grequests.map(rs, exception_handler=exception_handler,
                      gtimeout=TIMEOUT_TIME)
        logging.debug("putFoward Finished")
    else:
        logging.debug(
            "request is from a replica (not a client), not fowarding")


def exception_handler(request, exception):
    logging.warning("Replica may be down! Timeout on address: %s", request.url)
    repairView(request.url)


def retrieveStore():
    logging.warning("Running Retrieve Store")


def repairView(downSocket):
    global view
    logging.warning("%s is down, adjusting view", downSocket)
    view.remove(downSocket)
    app.forward(None, downSocket, True, "VIEW_DELETE")


# TODO: views startup
@app.on_event('startup')
async def startup():
    retrieveStore()

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.background import BackgroundTask
from starlette.endpoints import HTTPEndpoint
import uvicorn
# import requests

import logging
import asyncio
import random


logging.basicConfig(level=logging.DEBUG)

app = Starlette(debug=True)

kvs = {}
history = []

# TODO: cmLists and ValueStore, delete, views, fowarding

class ValueStore:
    def __init__(self, value, version, cm):
        self.versions = []
        self.values = []
        self.causalMetadata = cm
        self.update(value, version)
    def getValue(self,index=-1):
        return self.values[index]
    def getVersion(self,index=-1):
        return self.versions[index]
    def update(self,version,value):
        self.values.append(value)
        self.versions.append(version)
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

@app.route('/key-value-store/{key}')
class KeyValueStore(HTTPEndpoint):
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

        if len(key) > 50: # key
            message = {"error": "Key is too long",
                        "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'value' in data: # value
            value = data['value']
        else:
            message = {"error": "Value is missing", "message": "Error in PUT"}
            return JSONResponse(message, status_code=400, media_type='application/json')
        if 'version' in data: # version
            version = data['version']
        else:
            version = random.randint(0,1000)
            logging.info("No Version in data, generating a unique version id: %s", version)
        if 'causal-metadata' in data: #causalMetadata
            causalMetadata = data['causal-metadata']
        logging.debug("===Put at %s : %s", key, value)

        # Second, we set up the task that will update the value
        # https://www.starlette.io/background/

        vs = ValueStore(value, version, causalMetadata.copy())
        task = BackgroundTask(putData, key=key, vs=vs, causalMetadata=causalMetadata)

        # Finally we return
        causalMetadata.append(version)
        if key in kvs:
            message = {
                "message": "Updated successfully",
                "version": version,
                "causal-metadata": causalMetadata
            }
            return JSONResponse(message, status_code=200, background=task, media_type='application/json')
        else:
            message = {
                "message": "Added successfully",
                "version": version,
                "causal-metadata": causalMetadata
            }
            return JSONResponse(message, status_code=201, background=task, media_type='application/json')
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


async def putData(key, vs, causalMetadata):
    value = vs.getValue()
    version = vs.getVersion()
    logging.debug("+++PutData started at:\n   Key: %s\n   Value: %s\n   CM:%s", key, value, causalMetadata)
    while(not isInCausalOrder):
        logging.debug("%s not in %s, waiting request", causalMetadata, history)
        await asyncio.sleep(5)
    kvs[key] = vs
    history.append(version) 
    logging.info("Added %s to history", version)
    logging.debug("New History: %s", history)
    logging.info("---PutData finished with kvs updated: %s => %s", key, kvs[key].getValue())


def isInCausalOrder(causalMetadata):
    if(causalMetadata):
        logging.debug("Request *is* causally dependent on: %s", causalMetadata)
        for v in causalMetadata:
            if v not in history:
                logging.warning("Request *is* not in causal order!")
                return False
    return True

async def putFoward(key, value):
    logging.debug("---PutFoward at:\n   Key: %s\n   Value: %s\n", key, value)




if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)

from flask import Flask
from flask import request
from flask import jsonify
from flask import Response
from flask import json
import threading
import time
import os
import requests
import random


class valueStore:
    def __init__(self, value, version):
        self.versions = []
        self.values = []
        self.causalMetadata = []
        self.update(version, value)
    def getMetadata(self):
        return ",".join(self.causalMetadata)
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
  


    
class kvsEncoder(json.JSONEncoder):
    def default(self,obj):
        if isinstance(obj, valueStore):
            return {"value":obj.values,"version" : obj.versions,"causalMetadata" : obj.causalMetadata}
        return kvsEncoder.default(self,obj)

    def generate(self):
        # self.casualMetadata.append(self.version)
        self.version = self.version + 1


# Globals
history = [] # Contains a list of version ids of already run commands
kvs = {}
kvs_version = {}
view = os.environ['VIEW']
viewArray = view.split(",")
replicaEnviroment = os.environ

app = Flask(__name__)
# Consts
BASE = 'http://'
KVS_ENDPOINT = '/key-value-store/'
VIEW_ENDPOINT = '/key-value-store-view/'
OWN_SOCKET = os.environ['SOCKET_ADDRESS']
TIMEOUT_TIME = 3



# GET:
# Retuns the value at the key
# Get with no key returns the state
def getFunctd(key):
    # If Key empty, then we return the whole KVS
    print("===== GET REQUEST")
    print()
    
    # If our kvs has the key
    #   we simply return the key
    # Otherwise,
    #   we send an error back (404)
    if key in kvs:
        print("Returning kvs version and value current lists:")
        print("value")
        print(kvs[key].getValue())
        print("versions")
        print(kvs[key].getVersion())
        resp = {"doesExist": True,
                "message": "Retrieved successfully",
                "value": kvs[key].getValue(),
                "version": kvs[key].getVersion(),
                "causal-metadata": kvs[key].causalMetadata}
        return Response(response=json.dumps(resp), status=200, mimetype='application/json')
    else:
        resp = {"doesExist": False,
                "error": "Key does not exist",
                "message": "Error in GET"}
        return Response(response=json.dumps(resp), status=404, mimetype='application/json')


# PUT:
# Handles put requests from client and
# runs requests in casual order
def putFunctd(key):
    # Set up variables for later use
    global viewArray
    global view
    global history
  
    value = ""
    causalMetadata = []
    version = ""
    data = request.get_json()
    isUpdating = key in kvs

    # Retreive passed info from data json
    # Check key length
    if len(key) > 50:
        resp = {"error": "Key is too long",
                "message": "Error in PUT"}
        return Response(response=json.dumps(resp), status=400, mimetype='application/json')
    if 'value' in data:
        value = data['value']
    else:
        resp = {"error": "Value is missing", "message": "Error in PUT"}
        return Response(response=json.dumps(resp), status=400, mimetype='application/json')
    if 'causal-metadata' in data:
        causalMetadata = data['causal-metadata']
    if 'version' in data:   
        version = data['version']
    else:
        version = random.randint(0,1000)
        print("No Version in data, generating a unique version id: " + str(version))

    # Retreive Causal-metadata
    # if 'causal-metadata' in data:
    #   causalMetadata = data['causal-metadata']

    # If metadata is empty, then we know that the request is
    #   NOT causally dependent on any other PUT operation
    #       This means we need to create a new "causally defined"
    #       object.
    

    # However, when we have metadata, then our request *is*
    #   casually dependent on some other PUT operation
    #       This means we need to check to see if we have
    #       already run the dependent operation

    # However, when we have metadata, then our request *is*
    # casually dependent on some other PUT operation

    # The following code is for operations regardless of causal dependence:
    # First we Broadcast with new version to all other replicas
    #   Here we are also presented with the opportunity to check
    #   if the other replicas are still running, and make repairs
    #   to our view if necessary.
    # Then we respond to the client with the new version


    print("=====PUT")
    print()
    print("=====SENDER IP: "+request.remote_addr)
    senderSocket = request.remote_addr + ":8080"
    print()
    print("CURRENT VIEW")
    print(viewArray)
    print()

    if causalMetadata:  # Casually dependent  
        print("Request *is* casually dependent on:\n")
        print(causalMetadata)
        for v in causalMetadata:
            while not (v in history):
                print("Request is *not* in causal order!! Correcting...")
                time.sleep(2)
                #TODO: run this request later
                # Save version_id: (key, data, requester)         
    
    # Update or store key and value
    # Store version into history
    if isUpdating:         
        kvs[key].update(value, version)
    else:
        kvs[key] = valueStore(value, version)
    
    
    history.append(version)

    if senderSocket not in view.split(','): 
        forward(key, value, version, causalMetadata, "PUT")

    # Respond to Client
    kvs[key].causalMetadata.append(version)
    if isUpdating:
        resp = {
            "message": "Updated successfully",
            "version": version,
            "causal-metadata": kvs[key].causalMetadata
        }
        return Response(response=json.dumps(resp), status=200, mimetype='application/json')
    else:
        resp = {
            "message": "Added successfully",
            "version": version,
            "causal-metadata": kvs[key].causalMetadata
        }
        return Response(response=json.dumps(resp), status=201, mimetype='application/json')

# DELETE:
# TODO: make this work


def delFunctd(key):
   global history

   causalMetadata = []
   version = ""
   data = request.get_json()

   if 'causal-metadata' in data:
       causalMetadata = data['causal-metadata']
   if 'version' in data:
       version = data['version']
   else:
       version = random.randint(0, 1000)
       print("No Version in data, generating a unique version id: " + str(version))
   senderSocket = request.remote_addr + ":8080"

   if causalMetadata:  # Casually dependent
       print("Delete request *is* casually dependent on:\n")
       print(causalMetadata)
       for v in causalMetadata:
           if not (v in history):
               print("Request is *not* in causal order!! Correcting...")
               #TODO: run this request later
               # Save version_id: (key, data, requester)

   # Check if key exists
   #   Delete key
   # else
   #   return error
   if key in kvs:
       kvs[key].update(None, version)

       history.append(version)
       if senderSocket not in view.split(','):
           forward(key, None, version, causalMetadata, "DELETE")

       kvs[key].causalMetadata = causalMetadata.append(version)

       # Response
       resp = {"doesExist": True,
               "message": "Deleted successfully",
               "version": version,
               "causal-metadata": kvs[key].causalMetadata}

       # return that object within a Response object
       return Response(response=json.dumps(resp), status=200, mimetype='application/json')
   else:
       # Response
       resp = {"doesExist": False,
               "message": "Error in DELETE",
               "error": "Key does not exist"
               }
       jsonResp = json.dumps(resp)
       # return that object within a Response object
       return Response(response=jsonResp, status=404, mimetype='application/json')
# Returns the current view


def getView():
    # Return the current Replica's view
    resp = {"doesExist": True,
            "message": "View retrieved successfully",
            "view": view}
    jsonResp = json.dumps(resp)
    return Response(response=jsonResp, status=200, mimetype='application/json')


# Adds a new socket to the current view
def putView():
    global view
    # Retreive the new address
    # if the data is empty, error
    # if the data already exists in our view, error
    data = request.get_json(force=True)
    if 'socket-address' in data:
        newAddress = data['socket-address']
    else:
        resp = {"error": "Value is missing",
                "message": "Error in PUT"}
        return Response(response=json.dumps(resp), status=400, mimetype='application/json')
    if newAddress in view:
        resp = {"doesExist": True,
                "error": "Socket address already exists in the view",
                "message": "Error in PUT"}
        return Response(response=json.dumps(resp), status=404, mimetype='application/json')

    # Add to current view
    view += "," + newAddress

    # Return
    resp = {"doesExist": True,
            "message": "Replica added successfully to the view"}
    return Response(response=json.dumps(resp), status=200, mimetype='application/json')

# Deletes a given socket from the view


def deleteView():
    # Retreive the address to delete
    # If the data is empty, error
    global view
    global viewArray
    data = request.get_json(force=True)
    if 'socket-address' in data:
        delAddress = data['socket-address']
    else:
        # Case socket address is not in data
        resp = {"error": "Value is missing", "message": "Error in DELETE"}
        return Response(response=json.dumps(resp), status=400, mimetype='application/json')

    # Check if socket address does exist, delete and return
    if delAddress in viewArray:
        # Delete
        print("=====VIEW STRING BEFORE DELETION: " + view)
        viewArray.remove(delAddress)
        view = ","
        view = view.join(viewArray)
        print("DELADDRESS: "+delAddress)
        print()
        print("=====VIEW STRING AFTER DELETION: " + view)
        print("=====CURRENT VIEW AFTER DELETING: ")
        print(viewArray)
        print()

        # Return
        resp = {"doesExist": True,
                "message": "Replica deleted successfully from the view"}
        jsonResp = json.dumps(resp)
        return Response(response=jsonResp, status=200, mimetype='application/json')

    # Else socket is not in view, return error
    resp = {"doesExist": True,
            "error": "Socket address does not exist in the view",
            "message": "Error in DELETE"}
    jsonResp = json.dumps(resp)
    return Response(response=jsonResp, status=404, mimetype='application/json')


# Removes a down socket from our view
def repairView(downSocket):
    global view
    global viewArray
    # Remove the down socket from own view
    print(downSocket + " IS DOWN. ADJUSTING VIEW.")
    viewArray.remove(downSocket)
    view = ","
    view = view.join(viewArray)
    print("=====DELADDRESS: "+downSocket)
    print()
    print("=====CURRENT VIEW AFTER DELETING: ")
    print(viewArray)
    print()

    # Broadcast delete
    for address in viewArray:

        if address.split(":")[0] != OWN_SOCKET.split(":")[0]:
            print("====REPLICA BEING SENT DELETE REQUEST: "+address)
            print("====REPLICA SENDING DELETE REQUEST: "+OWN_SOCKET)
            requests.delete(
                BASE + address + VIEW_ENDPOINT,
                timeout=TIMEOUT_TIME,
                json={'socket-address': downSocket}
            )
            print("=====DELETE VIEW REQUEST SENT TO "+address)

@app.before_first_request
def onCreate():
    global viewArray
    global view
    global kvs
    print("onCreate")
    print(viewArray)
    try:
        for address in viewArray:
            if address.split(":")[0] != OWN_SOCKET.split(":")[0]:
                requests.put(
                    BASE + address + VIEW_ENDPOINT,
                    timeout=TIMEOUT_TIME,
                    json={'socket-address': OWN_SOCKET}
                )
        if not kvs:
            print("====ATTEMPTING TO RETREIVE STORE FROM "+viewArray[0])
            storeResponse = requests.get(
                BASE + viewArray[0] + KVS_ENDPOINT,
                timeout=TIMEOUT_TIME
            )
            storeData = storeResponse.json()
            if 'store' in storeData:
                kvsDict = json.loads(storeData['store'])
                kvs = {}
                for key in kvsDict:
                    kvs[key] = valueStore(1,1)
                    kvs[key].values = kvsDict[key]['value']
                    kvs[key].versions = kvsDict[key]['version']
                    kvs[key].casualMetadata = kvsDict[key]['causalMetadata']
                print(kvs)
          


       
    except (requests.Timeout, requests.ConnectionError, ValueError):
        print("timeout onCreate")

# For when a new instance comes online
def insertView(newSocket):
    global kvs
    # Broadcast Put
    for address in viewArray:
        if address.split(":")[0] != OWN_SOCKET.split(":")[0]:
            requests.put(
                BASE + address + VIEW_ENDPOINT,
                timeout=TIMEOUT_TIME,
                json={'socket-address': newSocket}
            )
    # Broadcast Get KVS
    # parse string into dictionary
    if not kvs:
        print("====ATTEMPTING TO RETREIVE STORE FROM "+viewArray[0])
        storeResponse = requests.get(
            BASE + viewArray[0] + KVS_ENDPOINT,
            timeout=TIMEOUT_TIME
        )
        storeData = storeResponse.json()
        if 'store' in storeData:
            kvsDict = json.loads(storeData['store'])
            kvs = {}
            for key in kvsDict:
                kvs[key] = valueStore(1,1)
                kvs[key].values = kvsDict[key]['value']
                kvs[key].versions = kvsDict[key]['version']
                kvs[key].casualMetadata = kvsDict[key]['causalMetadata']
            print(kvs)

# Runs commands in queue that
# are in causal order


def runWaitingCommands():
    print("doesn't work yet")
    # loop through queue
    # run put(x.key)



def forward(key, value, version, causalMetadata, request):
   global viewArray
   viewArray = view.split(",")
   print("=====FORWARDING PUT REQUEST")
   print()
   print("=====CURRENT VIEW BELOW ")
   print(viewArray)
   print()
   for address in viewArray:
       if address.split(":")[0] != OWN_SOCKET.split(":")[0]:
           print("=====CURRENT ITERATING ADDRESS BEING FORWARDED TO: " +
                 address.split(":")[0])
           print()
           print("=====CURRENT REPLICA ADDRESS FORWARDING REQUEST: " +
                 OWN_SOCKET.split(":")[0])
           print()
           try:
               if request == "PUT":
                   forwarded = requests.put(
                       BASE + address + KVS_ENDPOINT + key,
                       timeout=TIMEOUT_TIME,
                       json={'value': value,
                             'version': version,
                             'causal-metadata': causalMetadata}
                   )
               elif request == "DELETE":
                   forwarded = requests.delete(
                       BASE + address + KVS_ENDPOINT + key,
                       timeout=TIMEOUT_TIME,
                       json={'version': version,
                             'causal-metadata': causalMetadata}
                   )
               #   return forwarded.content, forwarded.status_code
           except (requests.Timeout, requests.ConnectionError):  # Timeout, view repairs needed
               print("Warning: Replica may be down! Timeout on address: ")
               print(address)
               repairView(address)
           print("====FORWARDED PUT SENT TO: "+address)
   return forwarded.content, forwarded.status_code


# Operations for the view endpoint
@app.route("/key-value-store-view/", methods=['GET', 'PUT', 'DELETE'])
def viewResp():
    # GET.
    if request.method == 'GET':
        return getView()
    # PUT.
    elif request.method == 'PUT':
        return putView()
    # DELETE.
    elif request.method == 'DELETE':
        return deleteView()

# Main method. Handles request type, and determines if the request is intended for forwarding instance
# or the main instance.
@app.route("/key-value-store/<key>", methods=['PUT', 'GET', 'DELETE'])
def kvsresp(key):
    # GET.
    if request.method == 'GET':
        return getFunctd(key)
    # PUT.
    elif request.method == 'PUT':
        return putFunctd(key)
    # Handling for DELETE.
    elif request.method == 'DELETE':
        return delFunctd(key)
    runWaitingCommands()
@app.route("/key-value-store/", methods=['GET'])
def storeresp():
        return getStore()
def getStore():
    global kvs
    resp = {"store": json.dumps(kvs, cls=kvsEncoder)}
    jsonResp = json.dumps(resp)
    return Response(response=jsonResp,status=200, mimetype='application/json')



#
#adapted from example on https://networklore.com/start-task-with-flask/
#
def jump_starter():
    def starter():
        started = False
        while not started:
            try:
                r = requests.get(BASE + OWN_SOCKET + VIEW_ENDPOINT)
                if r.status_code == 200:
                    started = True
            except:
                print('Server not yet started')
            time.sleep(2)
    thread = threading.Thread(target=starter)
    thread.start()

if __name__ == "__main__":
    #app.before_first_request(onCreate)
    jump_starter()

    app.run(host='0.0.0.0', port=8080, threaded=True)

    # This needs to run when we boot a new process.
    print("WHYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
    

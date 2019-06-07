import logging
import asyncio

import json
import jsonpickle

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


# Globals
kvs = {}
history = []


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




          







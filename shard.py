import os
import json
import random
import logging
import math
import hashlib

class ReplicaGroup:
    def __init__(self, node_id, count, members, keys, fingerTable):
        self.shard_id = node_id
        self.shard_count = count
        self.shard_id_members = members
        self.key_count = keys
        self.finger_table = {}

    def getReplicaGroupID(self):
        return self.shard_id

    def getReplicaGroupCount(self):
        return self.shard_count

    def getReplicas(self):
        return self.shard_id_members

    def getCountOfReplicas(self):
        return self.keys

def lookup(hashed):
    logging.debug("Looking up replica group for " + hashed)
    groupID = hash(hashed) % 2 
    logging.debug(hashed + "to be put at group:"+ str(groupID))
    return groupID
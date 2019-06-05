import os
import json
import random
import logging
import math
import hashlib
import app

class ReplicaGroup:
    def __init__(self, node_id, count, members, keys, fingerTable):
        self.shard_id = node_id
        self.shard_count = count
        self.shard_id_members = []
        self.key_count = keys
        self.finger_table = {}
        self.successor = 1 if node_id == 2 else 2

    def getReplicaGroupID(self):
        return self.shard_id
    def getReplicaGroupCount(self):
        return self.shard_count
    def getReplicas(self):
        tempString =",".join(self.shard_id_members)
        logging.debug(tempString)
        return tempString
    def getCountOfKeys(self):
        return self.key_count
    def incrementKeyCount(self):
        self.key_count +=1
    def addGroupMember(self,addr):
        logging.debug("adding %s to replica group %s",addr,self.shard_id)
        self.shard_id_members.append(addr)
        logging.debug("replica group %s members: %s",self.shard_id,self.shard_id_members)
    

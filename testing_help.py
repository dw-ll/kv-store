import unittest
import requests
import time
import os

#######
# Run with:
# python -i .\testing_help.py


# Get on replica1
# response = requests.get( 'http://localhost:8082/key-value-store-view')
# response = requests.put('http://localhost:8082/key-value-store/mykey1', json={"value": "myvalue1", "causal-metadata": ""})



######################## initialize variables ################################################
subnetName = "assignment4-net"
subnetAddress = "10.10.0.0/16"

nodeIpList = ["10.10.0.2", "10.10.0.3", "10.10.0.4", "10.10.0.5", "10.10.0.6", "10.10.0.7"]
nodeHostPortList = ["8082","8083", "8084", "8085", "8086", "8087"]
nodeSocketAddressList = [ replicaIp + ":8080" for replicaIp in nodeIpList ]

view = ""
for nodeSocketAddress in nodeSocketAddressList:
    view += nodeSocketAddress + ","
view = view[:-1]

shardCount = 2

############################### Docker Linux Commands ###########################################################
def removeSubnet(subnetName):
    os.system("echo y | docker network prune")
    command = "docker network rm " + subnetName
    os.system(command)
    time.sleep(2)

def createSubnet(subnetAddress, subnetName):
    command  = "docker network create --subnet=" + subnetAddress + " " + subnetName
    os.system(command)
    time.sleep(2)

def buildDockerImage():
    command = "docker build -t assignment4-img ."
    os.system(command)

def runInstance(hostPort, ipAddress, subnetName, instanceName):
    command = "docker run -d -p " + hostPort + ":8080 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " -e SOCKET_ADDRESS=" + ipAddress + ":8080" + " -e VIEW=" + view + " -e SHARD_COUNT=" + str(shardCount) + " assignment4-img"
    os.system(command)
    time.sleep(2)
    
def runAdditionalInstance(hostPort, ipAddress, subnetName, instanceName, newView):
    command = "docker run -d -p " + hostPort + ":8080 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " -e SOCKET_ADDRESS=" + ipAddress + ":8080" + " -e VIEW=" + newView  + " assignment4-img"
    os.system(command)
    time.sleep(2)

def stopAndRemoveInstance(instanceName):
    stopCommand = "docker stop " + instanceName
    removeCommand = "docker rm " + instanceName
    os.system(stopCommand)
    time.sleep(2)
    os.system(removeCommand)

def connectToNetwork(subnetName, instanceName):
    command = "docker network connect " + subnetName + " " + instanceName
    os.system(command)

def disconnectFromNetwork(subnetName, instanceName):
    command = "docker network disconnect " + subnetName + " " + instanceName
    os.system(command)


def quick():
    print("###################### Building Docker Image ######################\n")
    # build docker image
    buildDockerImage()

    print("\n###################### Creating the subnet ######################\n")
    # remove the subnet possibly created from the previous run
    removeSubnet(subnetName)

    # create subnet
    createSubnet(subnetAddress, subnetName)
    
    print("\n###################### Running Instances ######################\n")
    runInstance(nodeHostPortList[0], nodeIpList[0], subnetName, "node1")
    
    print("--End quick")


def setup():
    print("###################### Building Docker Image ######################\n")
    # build docker image
    buildDockerImage()

    stopAndRemoveInstance("node1")


    print("\n###################### Creating the subnet ######################\n")
    # remove the subnet possibly created from the previous run
    removeSubnet(subnetName)

    # create subnet
    createSubnet(subnetAddress, subnetName)


    print("\n###################### Running Instances ######################\n")
    runInstance(nodeHostPortList[0], nodeIpList[0], subnetName, "node1")
    runInstance(nodeHostPortList[1], nodeIpList[1], subnetName, "node2")
    runInstance(nodeHostPortList[2], nodeIpList[2], subnetName, "node3")
    runInstance(nodeHostPortList[3], nodeIpList[3], subnetName, "node4")
    runInstance(nodeHostPortList[4], nodeIpList[4], subnetName, "node5")
    runInstance(nodeHostPortList[5], nodeIpList[5], subnetName, "node6")


    print("--End SetupEnvironment")
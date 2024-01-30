#*coding=utf-8
from __future__ import annotations
import os
from abc import ABC,abstractmethod
from rocketmq.client import Producer,Message,PushConsumer,ConsumeStatus

def updateTopic(Broker:str,NameServer:str,Topic:str,ClusterName="DefaultCluster"):
    os.system("/usr/bin/mqadmin updateTopic -b {broker} -n {nameserver} -t {topic}".format(broker=Broker,nameserver=NameServer,topic=Topic))

def topicList(NameServer:str):
    os.system("/usr/bin/mqadmin topicList -n {nameserver} -c".format(nameserver=NameServer))

def updateSubGroup(Broker:str,NameServer:str,GroupName:str,ClusterName="DefaultCluster"):
    os.system("/usr/bin/mqadmin updateSubGroup -b {broker} -n {nameserver} -g {groupname} -c {clustername}".format(broker=Broker,nameserver=NameServer,groupname=GroupName,clustername=ClusterName))

def clusterList(NameServer:str):
    os.system("/usr/bin/mqadmin clusterList -n {nameserver}".format(nameserver=NameServer))

def statsAll(NameServer:str):
    os.system("/usr/bin/mqadmin statsAll -n {nameserver}".format(nameserver=NameServer))

if __name__ == "__main__":
    print("Hello from mqadmin-cmd")


#*coding=utf-8
from __future__ import annotations
import os
import logging
from abc import ABC,abstractmethod
from rocketmq.client import Producer,Message,PushConsumer,ConsumeStatus

#所有命令的抽象类
class Command(ABC):
    @abstractmethod
    def execute(self)->None:
        pass

#创建producer命令
class CreateProducerCommand(Command):
    def __init__(self,createproducer:CreateProducer,producerid:str,namesrv:str)->None:
        self._createproducer = createproducer
        self._producerid = producerid
        self._namesrv = namesrv

    def execute(self) -> None:
        self._createproducer.create(self._producerid,self._namesrv)

        return self._createproducer.get_producer()

#创建producer的具体实现
class CreateProducer:
    def create(self,producerid:str,namesrv:str):
        self._producer = Producer(producerid)
        self._producer.set_name_server_address(namesrv)

    def get_producer(self):
        return self._producer

#创建Message的命令
class CreateMessageCommand(Command):
    def __init__(self,createmessage:CreateMessage,topic:str,message:str)->None:
        self._createmessage = createmessage
        self._message = message
        self._topic = topic

    def execute(self) -> None:
        return self._createmessage.create(self._topic,self._topic,self._message)


#创建Message的具体实现
class CreateMessage:
    def create(self,producer:Producer,topic:str,message:str):
        self._producer = producer
        self._topic = topic

        self._message = Message(self._topic)
        self._message.set_keys('xxx')
        self._message.set_tags('xxx')
        self._message.set_property('property','test')
        self._message.set_body(message)

        return self._message

#创建一个consumer
class CreateConsumerCommand(Command):
    def __init__(self,createconsumer:CreateConsumer,group:str,nameserver:str,topic:str)->None:
        self._createconsumer=createconsumer
        self._group = group
        self._namesrv = nameserver
        self._topic = topic

    def execute(self) -> None:
        return self._createconsumer.create(self._group,self._namesrv,self._topic)

#创建consumer的具体实现
class CreateConsumer:
    def _callback(self,msg):
        if not os.path.exists('./consumerLogging'):
            open('./consumerLogging',"x")
        else:
            open('./consumerLogging',"w")

        logging.basicConfig(filename="./consumerLogging",level=logging.DEBUG)
        #logging.debug(msg.id,msg.body,msg.get_property('property'))
        logging.debug(msg.id)
        logging.debug(msg.body)
        return ConsumeStatus.CONSUME_SUCCESS

    def create(self,group:str,nameserver:str,topic:str) -> None:
        self._consumer = PushConsumer(group)
        self._consumer.set_name_server_address(nameserver)
        self._consumer.subscribe(topic,self._callback)
        self._consumer.start()

        return self._consumer

class Invoker:
    _command = None
    def set_command(self,command:Command):
        self._command = command

    def do_command(self) -> None:
        if isinstance(self._command,Command):
            return self._command.execute()

if __name__ == "__main__":
    invoker = Invoker()

    crp = CreateProducer()
    msg = CreateMessage()
    con = CreateConsumer()
    
    invoker.set_command(CreateProducerCommand(crp,"PID-1","192.168.10.129:9876"))
    producer = invoker.do_command()

    invoker.set_command(CreateMessageCommand(msg,"TestTopic","Hello"))
    mqmsg = invoker.do_command()

    invoker.set_command(CreateConsumerCommand(con,"Test","192.168.10.129:9876","TestTopic"))
    mqcon = invoker.do_command()

    producer.start()
    ret = producer.send_sync(mqmsg)
    print(ret.status,ret.msg_id,ret.offset)
    producer.shutdown()


#*coding=utf-8
import os
import random
import time
import command
import threading
from datetime import datetime
from rocketmq.client import Producer,Message

#创建消费者消费消息
def consume(group:str,nameserver:str,topic:str):
    consumer = command.CreateConsumer()
    invoker = command.Invoker()

    invoker.set_command(command.CreateConsumerCommand(consumer,group,nameserver,topic))
    mqconsumer = invoker.do_command()

    mqconsumer.start()

def pull_consume(group:str,nameserver:str,topic:str):
    consumer = command.CreatePullConsumer()
    invoker = command.Invoker()

    invoker.set_command(command.CreatePullConsumerCommand(consumer,group,nameserver,topic))
    mqconsumer = invoker.do_command()

    mqconsumer.start()

#创建producer并产生消息
def produce(producerid:str,nameserver:str,topic:str,message:str):
    msg = command.CreateMessage()
    pro = command.CreateProducer()
    invoker = command.Invoker()

    invoker.set_command(command.CreateProducerCommand(pro,producerid,nameserver))
    producer = invoker.do_command()

    invoker.set_command(command.CreateMessageCommand(msg,topic,message))
    mqmsg = invoker.do_command()

    producer.start()
    ret = producer.send_sync(mqmsg)
    producer.shutdown()

#生产多个消息
def produceMultiple(producerid:str,nameserver:str,topic:str,number:int):
    pro = command.CreateProducer()
    invoker = command.Invoker()
    invoker.set_command(command.CreateProducerCommand(pro,producerid,nameserver))
    producer = invoker.do_command()
    producer.start()
    for i in range(0,number):
        msg = command.CreateMessage()
        invoker.set_command(command.CreateMessageCommand(msg,topic,str(number)))
        mqmsg = invoker.do_command()
        print("send {message} to {sendtopic}".format(message=str(number),sendtopic=topic))
        producer.send_sync(mqmsg)
    producer.shutdown()

group_topics = {"TestTopic":"GT1","TestTopic1":"GT2","TestTopic2":"GT3"}

def Schedule():

    rd_msg = random.uniform(100,1000)

    while True:
        for t,g in group_topics.items():
            produceMultiple(str(datetime.now()),"127.0.0.1:9876",t,int(round(rd_msg)))
            time.sleep(60)
            consume(g,"127.0.0.1:9876",t)
            time.sleep(60)

commandTips={}
commandTips["mqadmin updateSubGroup"]=("#创建/更新订阅关系\nmqadmin updateSubGroup [broker-address] [name-server] [group name] [clustername]")
commandTips["mqadmin deleteSubGroup"]=("#删除订阅关系\nmqadmin deleteSubgroup [broker-address] [name-server] [group name] [clustername]")
commandTips["updatetopic"]=("#创建/更新Topic\nupdatetopic [broker-address] [name-server] [topic name] [clustername]")
commandTips["produceMultiple"]=("#制作N个消息\nproduceMultiple [produceid] [name-server] [topic] [message number]")
commandTips["produce"]=("#制作消息\nproduce [produceid] [name-server] [topic] [message]")
commandTips["consume"]=("#消费消息\nconsume [consum-group] [name server] [topic]")
commandTips["mqadmin clusterList"]=("#列出集群信息\nmqadmin clusterList -n [name-server]")
commandTips["mqadmin topicList"]=("#列出topic列表\nmqadmin topicList -n [name-server]")
commandTips["mqadmin statsAll"]=("#展示集群状态\nmqadmin statsAll -n [name-server]")
commandTips["scheule"]=("#随机创建Topic并投递到RocketMQ中")
commandTips["help"]=("#展示此信息\nhelp")


def loop():
    while True:
        user_input = input("Rocketmq Client > ")
        user_input = user_input.split() #user_input的第一个元素是命令
        cmd = user_input[0]

        if cmd.lower() == "exit":
            break
        try:
            if cmd == "consume":
                if len(user_input) != 4:
                    print("consume [consum-group] [name server] [topic]")
                    continue
                else:
                    t = threading.Thread(target=consume,args=(user_input[1],user_input[2],user_input[3]))
                    t.start()
                    t.join()
            elif cmd == "updatetopic":
                if len(user_input) != 5 :
                    print("updatetopic [broker-address] [name-server] [topic name] [clustername]")
                    continue
                else:
                    mqc.updateTopic(user_input[1],user_input[2],user_input[3],user_input[4])
            elif cmd == "updatesubgroup":
                if len(user_input) != 5 :
                    print("updatesubgroup [broker-address] [name-server] [group name] [clustername]")
                    continue
                else:
                    mqc.updateSubGroup(user_input[1],user_input[2],user_input[3],user_input[4])
            elif cmd == "produce":
                if len(user_input) != 5 :
                    print("produce [produceid] [name-server] [topic] [message]")
                    continue
                else:
                    produce(user_input[1],user_input[2],user_input[3],user_input[4])
            elif cmd == "produceMultiple":
                if len(user_input) != 5 :
                    print("produceMultiple [produceid] [name-server] [topic] [messagenumber]")
                    continue
                else:
                    produceMultiple(user_input[1],user_input[2],user_input[3],int(user_input[4]))
            elif cmd == "scheule":
                Schedule()

            elif cmd == "help":
                for value in commandTips.values():
                    print(value+"\n")

            else:
                result = os.popen(" ".join(user_input)).read()
                print(result)
        except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    print("Simple Shell - Type 'exit' to quit\n")
    print("Command usage list:\n")

    for key,value in commandTips.items():
        print(value+"\n")
    loop()

    #Schedule()

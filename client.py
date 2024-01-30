#*coding=utf-8
import os
import command
import threading
import mqadmin_cmd as mqc
from rocketmq.client import Producer,Message

def consume(group:str,nameserver:str,topic:str):
    consumer = command.CreateConsumer()
    invoker = command.Invoker()

    invoker.set_command(command.CreateConsumerCommand(consumer,group,nameserver,topic))
    mqconsumer = invoker.do_command()

    mqconsumer.start()

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


def loop():
    while True:
        user_input = input("Rocketmq Client > ")
        user_input = user_input.split()
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
            elif cmd == "topiclist":
                if len(user_input) != 2 :
                    print("topiclist [name-server]")
                    continue
                else:
                    mqc.topicList(user_input[1])
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

            elif cmd == "clusterlist":
                if len(user_input) != 2 :
                    print("clusterList [name-server]")
                    continue
                else:
                    mqc.clusterList(user_input[1])

            elif cmd == "statsall":
                if len(user_input) != 2 :
                    print("clusterList [name-server]")
                    continue
                else:
                    mqc.statsAll(user_input[1])
            elif cmd == "produce":
                if len(user_input) != 5 :
                    print("produce [produceid] [name-server] [topic] [message]")
                    continue
                else:
                    produce(user_input[1],user_input[2],user_input[3],user_input[4])

            else:
                result = os.popen(" ".join(user_input)).read()
                print(result)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    print("Simple Shell - Type 'exit' to quit")
    loop()

#*coding=utf-8
import os
import command
import threading
from rocketmq.client import Producer,Message

def consume(group:str,nameserver:str,topic:str):
    consumer = command.CreateConsumer()
    invoker = command.Invoker()

    invoker.set_command(command.CreateConsumerCommand(consumer,group,nameserver,topic))
    mqconsumer = invoker.do_command()

    mqconsumer.start()

def produce():
    print("This is produce")

cmdList = {'consume':consume,'produce':produce}

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
               
            elif cmd == "produce":
                produce()
            else:
                result = os.popen(" ".join(user_input)).read()
                print(result)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    print("Simple Shell - Type 'exit' to quit")
    loop()

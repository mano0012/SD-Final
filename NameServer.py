import socket
import threadPool
import pickle
import json
import queue

MAX_THREADS = 100
THREAD_BLOCK = 10

class DNS:
    def __init__(self):
        self.ip = "127.0.0.1"
        self.port = 10001
        self.services = ["SQL", "CHAT"]
        self.serverList = self.services.copy()

        # Socket UDP
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.s.bind((self.ip, self.port))

        for i in range(len(self.services)):
            self.serverList[i] = queue.Queue()

        self.threads = threadPool.tPool(self.getAddress, MAX_THREADS, THREAD_BLOCK)

        print("DNS is set up")

    def run(self):
        while True:
            data, addr = self.s.recvfrom(1024)

            t = self.threads.getThread([data, addr])

            t.start()

    def getServicesList(self):
        return self.services

    #Terminar getAddress
    def getAddress(self, data, address):
        message = self.loadMessage(data)
        hasService = False

        if message == "getServices":
            self.sendToHost(address, self.services)
        else:
            for i in range(len(self.services)):
                if self.services[i] == message["type"]:
                    hasService = True
                    if message["con"] == "SERVER":
                        print("Adding ", message["type"], " server.")
                        self.addQueueSv(i, address)
                        print("DONE!")
                        self.sendToHost(address, "DONE!")
                    else:
                        svAddr = self.getServerAddress(i)

                        self.sendToHost(address, svAddr)

            if not hasService:
                self.sendToHost(address, "ERROR!")

        self.threads.repopulate()

    def getServerAddress(self, index):
        print("SELECTED SERVER: ", self.services[index])
        svAddr = self.removeQueueSv(index)

        if svAddr is not None:
            self.addQueueSv(index, svAddr)

        return svAddr

    def removeQueueSv(self, index):
        if self.serverList[index].empty():
            print("QUEUE EMPTY")
            return None

        return self.serverList[index].get()

    def addQueueSv(self, index, host):
        self.serverList[index].put(host)

    def convertJson(self, message):
        try:
            msg = json.dumps(message)
            return msg
        except:
            return message

    def loadJson(self, message):
        try:
            msg = json.loads(message)
            return msg
        except:
            return message

    def loadMessage(self, message):
        return self.loadJson(pickle.loads(message))

    def sendToHost(self, host, message):
        jsonMsg = self.convertJson(message)

        if jsonMsg is None:
            msgSerializada = pickle.dumps("ERROR!")
        else:
            msgSerializada = pickle.dumps(message)

        self.s.sendto(msgSerializada, host)

    def exit(self):
        self.s.close()

dns = DNS()
dns.run()

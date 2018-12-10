import socket
import json
import pickle

DNS_IP = "127.0.0.1"
DNS_PORT = 10001

class Client:
    def __init__(self):
        self.sock = None
        self.ip = "127.0.0.1"
        self.port = 9992
        self.serviceList = list()
        self.serviceAddr = None

        print("CLIENTE CRIADO")



    def createSocketUDP(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)

        self.sock.bind((self.ip, self.port))

    def closeSocket(self):
        try:
            self.sock.close()
        except:
            print("DEU NAO")
            pass

    def createSocketTCP(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_STREAM)  # TCP

    def run(self):
        op = 1

        self.createSocketUDP()

        while op != 0:
            op = self.getService()

            print("Serviços disponíveis")

            for i in range(len(self.serviceList)):
                print(i+1, "- ", self.serviceList[i])

            print("0 - Sair")

            op = int(input("Selecione o serviço: "))

            while op < 0 or op > len(self.serviceList):
                op = int(input("Serviço invalido, digite novamente: "))

            self.selectService(op)

    def connect(self):
        self.sock.connect(self.serviceAddr)

        print("\nConectado\n")

        msg = ""

        while msg != "exit":
            msg = input("> ")

            if msg != "exit":
                # Request
                self.sendTCP(self.prepareMsg(msg))

                "Reply"
                msg = self.loadMessage(self.sock.recv(1024))
                print(msg)

        print("\n")

    def selectService(self, service):
        if service == 0:
            print("GOODBYE!")
        else:
            self.sendUDP((DNS_IP, DNS_PORT), self.prepareMsg({"con":"CLIENTE", "type":self.serviceList[service-1]}))
            data, _ = self.sock.recvfrom(1024)

            self.serviceAddr = self.loadMessage(data)

            self.closeSocket()

            self.createSocketTCP()

            self.connect()

    def getService(self):
        self.closeSocket()

        self.createSocketUDP()

        self.sendUDP((DNS_IP, DNS_PORT), self.prepareMsg("getServices"))
        data, _ = self.sock.recvfrom(1024)

        self.serviceList = self.loadMessage(data)

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

    def prepareMsg(self, msg):
        jsonMsg = self.convertJson(msg)

        serializedMsg = pickle.dumps(jsonMsg)

        return serializedMsg

    def sendTCP(self, serializedMsg):
        self.sock.send(serializedMsg)

    def sendUDP(self, host, serializedMessage):
        self.sock.sendto(serializedMessage, host)

cliente = Client()
cliente.run()
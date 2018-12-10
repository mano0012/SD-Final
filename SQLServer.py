import socket
import threadPool
import json
import pickle
import mysql.connector

MAX_THREADS = 100
THREAD_BLOCK = 10
DNS_IP = "127.0.0.1"
DNS_PORT = 10001

class sqlServer:
    def __init__(self):
        self.ipAddress = "127.0.0.1"
        self.port = 9991
        self.sock = None

        self.threads = threadPool.tPool(self.run, MAX_THREADS, THREAD_BLOCK)

        self.createSocketUDP()

        self.callDNS({"con": "SERVER", "type": "SQL"})

        self.closeSocket()

        self.createSocketTCP()

        self.startDB()


    def startDB(self):
        sqlConnector = mysql.connector.connect(
            host="localhost",
            user="stick",
            passwd="014412",
            database="sqlDB"
        )

        cursor = sqlConnector.cursor()

        try:
            cursor.execute("CREATE TABLE funcionarios (login VARCHAR(255) PRIMARY KEY NOT NULL, senha VARCHAR(255) NOT NULL)")
        except:
            pass

        try:
            cursor.execute("CREATE TABLE produtos (id int PRIMARY KEY NOT NULL, nome VARCHAR(255) NOT NULL, qtd int NOT NULL)")
        except:
            pass

        try:
            sql = "INSERT INTO funcionarios (login, senha) VALUES (%s, %s)"
            val = ("Lucas", "123321")
            cursor.execute(sql, val)

            sql = "INSERT INTO funcionarios (login, senha) VALUES (%s, %s)"
            val = ("Matheus", "123456")
            cursor.execute(sql, val)

            sql = "INSERT INTO funcionarios (login, senha) VALUES (%s, %s)"
            val = ("Laura", "111111")
            cursor.execute(sql, val)
        except:
            pass

        sqlConnector.commit()

    def waitClient(self):
        while True:
            con, client = self.sock.accept()

            print("Cliente ", client, " conectado")
            t = self.threads.getThread([con])

            t.start()


    def createSocketUDP(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)

        self.sock.bind((self.ipAddress, self.port))

    def closeSocket(self):
        self.sock.close()

    def createSocketTCP(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_STREAM)  # TCP

        self.sock.bind((self.ipAddress, self.port))

        self.sock.listen(1)

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

    def sendToHost(self, host, serializedMessage):
        self.sock.sendto(serializedMessage, host)

    def callDNS(self, message):
        #self.sock.connect((DNS_IP, DNS_PORT))
        self.sendToHost((DNS_IP, DNS_PORT), self.prepareMsg(message))

        data, _ = self.sock.recvfrom(1024)

        print(self.loadMessage(data))

    def getMessage(self, connection):
        serializedMsg = connection.recv(1024)
        msg = self.loadMessage(serializedMsg)
        return msg

    def menu(self, connection, cursor, db):
        menu = "\nMenu\n1- Consultar estoque\n2- Inserir produto\n3- Remover produto\n0- Sair"
        connection.send(self.prepareMsg(menu))
        op = int(self.getMessage(connection))

        while op != 0:
            if op == 1:
                connection.send(self.prepareMsg("\nDigite o codigo do produto: "))
                cod = self.getMessage(connection)
                sql = ("SELECT * FROM produtos WHERE id=" + cod)
                print("SQL: ", sql)
                cursor.execute(sql)
                result = cursor.fetchall()
                if len(result) == 0:
                    connection.send(self.prepareMsg("\nEste produto nao foi cadastrado\n" + menu))
                else:
                    msg = "\nCodigo: " + str(result[0][0]) + " Nome: " + str(result[0][1]) + " QTD: " + str(result[0][2])
                    connection.send(self.prepareMsg(msg + "\n" + menu))
            elif op == 2:
                connection.send(self.prepareMsg("\nDigite o nome do produto: "))
                nome = self.getMessage(connection)

                connection.send(self.prepareMsg("\nDigite o codigo do produto: "))
                cod = self.getMessage(connection)

                connection.send(self.prepareMsg("\nDigite a quantidade do produto: "))
                qtd = self.getMessage(connection)

                sql = ("INSERT INTO produtos VALUES (" + cod + ", '" + nome + "', " + qtd + ")")

                cursor.execute(sql)

                print("SQL: ", sql)

                db.commit()

                connection.send(self.prepareMsg("\nProduto " + cod + " cadastrado\n" + menu))
            elif op == 3:
                connection.send(self.prepareMsg("\nDigite o codigo do produto: "))
                cod = self.getMessage(connection)

                sql = ("SELECT * FROM produtos WHERE id=" + cod)
                print("SQL: ", sql)
                cursor.execute(sql)

                result = cursor.fetchall()

                if len(result) == 0:
                    connection.send(self.prepareMsg("\nProduto não encontrado\n" + menu))
                else:
                    msg = "\nTem certeza que deseja remover o produto: Codigo: " + str(result[0][0]) + " Nome: " + str(result[0][1] + "\n"
                            "Digite S para sim ou N para não")

                    connection.send(self.prepareMsg(msg))

                    msg = self.getMessage(connection)

                    if msg == "S":
                        sql = "DELETE FROM produtos WHERE id=" + cod
                        cursor.execute(sql)
                        db.commit()
                        connection.send(self.prepareMsg("\nProduto Removido" + "\n" + menu))
                    else:
                        connection.send(self.prepareMsg("\nOperação cancelada" + "\n" + menu))

            op = int(self.getMessage(connection))

        connection.send(self.prepareMsg("exit"))

    def run(self, connection):
        sqlConnector = mysql.connector.connect(
            host="localhost",
            user="stick",
            passwd="014412",
            database="sqlDB"
        )

        cursor = sqlConnector.cursor()

        msg = self.getMessage(connection)

        if msg == "login":
            connection.send(self.prepareMsg("\nDigite seu login: "))
            login = self.getMessage(connection)
            connection.send(self.prepareMsg("\nDigite sua senha: "))
            passwd = self.getMessage(connection)

            sql = "SELECT * FROM funcionarios where login = %s and senha = %s"
            val = (login, passwd)

            cursor.execute(sql, val)

            results = cursor.fetchall()

            if len(results) == 1:
                self.menu(connection, cursor, sqlConnector)
            else:
                connection.send(self.prepareMsg("\nFalha na autenticação\n\n"))

        sqlConnector.close()

        self.threads.repopulate()


server = sqlServer()
server.waitClient()
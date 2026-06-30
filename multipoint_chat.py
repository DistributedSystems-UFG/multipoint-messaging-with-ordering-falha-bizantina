import time
import sys
import rpyc
import multiprocessing
import threading
import quotes
import random
import requests
import json
import heapq

NAME_SERVER_ADDR = "100.27.92.54"
NAME_SERVER_PORT = 18861
COMPARISON_SERVER_PORT = 18862

BASE_PEER_PORT = 18890  # BASE_PEER_PORT + peer id

N_MESSAGES = 25  # base de mensagens = 2508 lin2508 # base de mensagens = 2508 linhas


class MyPeer(rpyc.Service):
    def __init__(self, id, port, local=False) -> None:
        self.lock = threading.Lock()
        self.event_clock = 0  # relógio de lamport
        self.ack_count = {}  # ack para implementar o relógio
        self.start_event = threading.Event()
        self.id = id
        self.name = f"peer_{self.id}"
        self.port = port
        self.peers_conn = {}
        self.cs = None
        if local:
            self.ns_addr = "localhost"
            self.address = "localhost"
        else:
            self.address = requests.get("https://api.ipify.org").content.decode("utf8")
            self.ns_addr = NAME_SERVER_ADDR
        print(f"{self.name}: iniciando")

        self.t = threading.Thread(target=self.main)
        self.t.start()

    def main(self):
        self.connect_to_name_server()
        self.bind_myself_on_name_server()
        self.register_myself_on_name_server()
        self.loop()

    def loop(self):
        while True:
            self.start_event.clear()
            self.messages = random.sample(
                quotes.quote_list, N_MESSAGES
            )  # citaçoes aleatorias
            self.ready_peers = 0
            self.done_peers = 0
            self.start_event.wait()  # esperando server mandar o sinal
            if self.cs is None:
                self.connect_to_comparison_server()

            self.start_event.clear()
            if len(self.peers_conn) == 0:
                time.sleep(self.id)
                self.connect_to_all_peers()
            self.log = []  # log final. Elementos inseridos não serão relocados
            self.queue = []  # Fila de prioridade de mensagens esperando ACK
            self.send_ready_signal()
            self.start_event.wait()  # esperando outros peers ficarem prontos
            for message in self.messages:
                self.broadcast_message(f"{self.name}: {message}")
            self.start_event.clear()
            self.send_done_signal()
            self.start_event.wait()  # esperando outros peers terminarem
            self.send_log_to_server()

    def exposed_server_start_signal(self, n_messages):
        self.n_messages = n_messages
        print(f"{self.name}: server pediu multicast de {n_messages} mensagens")
        self.start_event.set()

    # print(f"{self.name}: ")
    def send_ready_signal(self):
        print(f"{self.name}: anunciando que estou pronto")
        for conn in self.peers_conn.values():
            conn.root.receive_ready_signal()

    def send_done_signal(self):
        print(f"{self.name}: anunciando que terminei")
        for conn in self.peers_conn.values():
            conn.root.receive_done_signal()

    def exposed_receive_ready_signal(self):
        # print(f"{self.name}: {name} está pronto")
        with self.lock:
            self.ready_peers += 1
        if self.ready_peers == len(self.peers_conn):
            print(f"{self.name}: todos estão prontos")
            self.start_event.set()

    def exposed_receive_done_signal(self):
        # print(f"{self.name}: {name} está pronto")
        with self.lock:
            self.done_peers += 1
        if self.done_peers == len(self.peers_conn):
            print(f"{self.name}: todos terminaram")
            self.start_event.set()

    def broadcast_message(self, message):
        print(f"{self.name}: broadcast {message}")
        with self.lock:
            self.event_clock += 1
        timestamped_message = [self.event_clock, self.id, message]
        for conn in self.peers_conn.values():
            conn.root.receive_message(json.dumps(timestamped_message))

    def exposed_receive_message(self, message):
        # print(f"{self.name}: received message {message}")
        timestamped_message = json.loads(message)  # [clock, id, message]
        with self.lock:
            self.event_clock = max(timestamped_message[0], self.event_clock) + 1
            heapq.heappush(self.queue, timestamped_message)
            self.ack_message(
                (timestamped_message[0], timestamped_message[1])
            )  # (clock, id)
            self.try_deliver()

    def try_deliver(self):
        # print(f"{self.name}: try_deliver()")
        while self.queue:
            msg = self.queue[0]
            message_signature = (msg[0], msg[1])
            if self.ackd_by_all(message_signature):
                heapq.heappop(self.queue)
                self.log.append(msg)
                del self.ack_count[message_signature]
            else:
                break

    def ackd_by_all(self, message_signature):
        # print(f"{self.name}: ackd_by_all()")
        return len(self.ack_count[message_signature]) == len(self.peers_conn)

    def ack_message(self, message_signature):  # ack implicito
        # print(f"{self.name}: ack_message()")
        self.ack_count[message_signature] = set()
        for message, ack_set in self.ack_count.items():
            if message[0] < message_signature[0]:
                ack_set.add(message_signature[1])

    def connect_to_name_server(self):
        print(
            f"{self.name}: conectando no name_server em {self.ns_addr},{NAME_SERVER_PORT}"
        )
        self.ns = rpyc.connect(self.ns_addr, NAME_SERVER_PORT)

    def connect_to_comparison_server(self):
        endpoint = self.ns.root.lookup("comparison_server")
        print(
            f"{self.name}: conectando no comparison_server em {endpoint[0]},{endpoint[1]}"
        )
        print("endpoing: ", endpoint)
        self.cs = rpyc.connect(endpoint[0], endpoint[1])

    def send_log_to_server(self):
        print(f"{self.name}: enviando log para o server")
        while self.queue:
            self.log.append(heapq.heappop(self.queue))
        log = json.dumps(self.log)
        assert self.cs is not None
        self.cs.root.receive_log(log)

    def bind_myself_on_name_server(self):
        print(f"{self.name}: fazendo binding no name_server")
        self.ns.root.bind(self.name, (self.address, self.port))

    def register_myself_on_name_server(self):
        self.ns.root.register(self.name, "peer")

    def update_peer_list(self):
        print(f"{self.name}: obtendo lista de pares")
        self.peers_info = json.loads(self.ns.root.discover("peer"))

    def connect_to_all_peers(self):
        self.update_peer_list()
        print(f"{self.name}: conectando com outros peers")
        self.peers_conn = {}
        for peer, endpoint in self.peers_info.items():
            conn = rpyc.connect(endpoint[0], endpoint[1])
            # print(f"{self.name}: conectando com {peer}")
            self.peers_conn[peer] = conn


class MyNameServer(rpyc.Service):
    def __init__(self) -> None:
        print("name_server: iniciando")
        self.peer_by_name = {}  # {nome: ("addr", port)}
        self.peer_by_type = {}  # {type: {nome: ("addr", port)}}

    def exposed_bind(self, name, address):
        """cria um registro nome-endereço; retorna o status (ok ou erro)"""
        self.peer_by_name[name] = address
        print(f"name_serer: {name} registrado como {address}")

    def exposed_lookup(self, name):
        """retorna o endereço associado ao nome (ou um erro, caso o nome não exista)"""
        return self.peer_by_name[name]

    def exposed_unbind(self, name):
        """remove um nome (e o registro associado)"""
        if name in self.peer_by_name:
            del self.peer_by_name[name]
        for peers in self.peer_by_type.values():
            if name in peers:
                del peers[name]

    def exposed_register(self, name, peer_type):
        """associa um tipo a um nome já registrado (bind) anteriormente; retorna um erro se nome não existir"""
        if name not in self.peer_by_name.keys():
            return "erro"
        addr = self.peer_by_name[name]
        if peer_type not in self.peer_by_type.keys():
            self.peer_by_type[peer_type] = {}
        self.peer_by_type[peer_type][name] = addr

    def exposed_discover(self, type):
        """retorna uma lista com todos os processos (nome e endereço) do tipo indicado"""
        return json.dumps(self.peer_by_type[type])


class MyComparisonServer(rpyc.Service):
    def __init__(self, local=False) -> None:
        print("comparison_server: iniciando")
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.port = COMPARISON_SERVER_PORT
        if local:
            self.ns_addr = "localhost"
            self.address = "localhost"
        else:
            self.address = requests.get("https://api.ipify.org").content.decode("utf8")
            self.ns_addr = NAME_SERVER_ADDR
        self.t = threading.Thread(target=self.main)
        self.t.start()

    def main(self):
        self.connect_to_name_server()
        self.bind_myself_on_name_server()
        self.connect_to_all_peers()
        self.loop()

    def loop(self):
        while True:
            self.event.clear()
            # n_messages = int(input("Insida a quantidade de mensagens: "))
            n_messages = N_MESSAGES
            self.logs = []
            self.send_start_signal(n_messages)
            self.logs_received = 0
            self.event.wait()  # esperando logs chegarem
            self.compare_logs()
            self.event.clear()
            self.event.wait()

    def bind_myself_on_name_server(self):
        print("comparison_server: fazendo binding no name_server")
        self.ns.root.bind("comparison_server", (self.address, self.port))

    def send_start_signal(self, n_messages):
        print(
            f"comparison_server: enviando sinal para peers com {n_messages} mensagens"
        )
        for conn in self.peers_conn.values():
            conn.root.server_start_signal(n_messages)

    def exposed_receive_log(self, log):
        print("comparison_server: recebendo log")
        with self.lock:
            self.logs.append(json.loads(log))
            self.logs_received += 1
        if self.logs_received == len(self.peers_conn):
            self.event.set()

    def connect_to_name_server(self):
        print(
            f"comparison_server: conectando no name_server em {self.ns_addr},{NAME_SERVER_PORT}"
        )
        self.ns = rpyc.connect(self.ns_addr, NAME_SERVER_PORT)

    def update_peer_list(self):
        print("comparison_server: obtendo lista de pares")
        self.peers_info = json.loads(self.ns.root.discover("peer"))

    def connect_to_all_peers(self):
        self.update_peer_list()
        print("comparison_server: conectando com outros peers")
        self.peers_conn = {}
        for peer, endpoint in self.peers_info.items():
            conn = rpyc.connect(endpoint[0], endpoint[1])
            print(f"comparison_server: conectando com {peer}")
            self.peers_conn[peer] = conn

    def compare_logs(self):
        # for msg in self.logs[0]:
        #     print(msg)
        unordered = 0
        lines = len(self.logs)
        colmn = len(self.logs[0])
        for j in range(colmn):
            msg = self.logs[0][j]
            for i in range(lines):
                if msg != self.logs[i][j]:
                    unordered += 1
        print(f"comparison_server: {unordered} mensagens fora de ordem")


def peer_process(id, is_local=False):
    from rpyc.utils.server import ThreadedServer

    p_port = BASE_PEER_PORT + id
    t = ThreadedServer(MyPeer(id, port=p_port, local=is_local), port=p_port)
    t.start()


def name_server_process():
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer(MyNameServer(), port=NAME_SERVER_PORT)
    t.start()


def comparison_server_process(is_local=False):
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer(MyComparisonServer(is_local), port=COMPARISON_SERVER_PORT)
    t.start()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(
            f"usage: python3 {sys.argv[0]} {{local N|comparison_server N|name_server|peer ID}}"
        )
    elif "local" in sys.argv:
        print("Rodando localmente")
        if len(sys.argv) != 3:
            print(f"python3 {sys.argv[0]} local N")
            exit()
        n_peers = int(sys.argv[2])

        ns = multiprocessing.Process(target=name_server_process)
        ns.start()

        time.sleep(1)
        peers = []
        for i in range(n_peers):
            peer = multiprocessing.Process(target=peer_process, args=(i, True))
            peer.start()
            peers.append(peer)

        time.sleep(1)
        cs = multiprocessing.Process(target=comparison_server_process, args=(True,))
        cs.start()

        ns.join()
        for peer in peers:
            peer.join()
        cs.join()

    elif "comparison_server" == sys.argv[1]:
        if len(sys.argv) != 2:
            print(f"python3 {sys.argv[0]} comparison_server")
            exit()
        comparison_server_process()
    elif "name_server" == sys.argv[1]:
        name_server_process()
    elif "peer" == sys.argv[1]:
        if len(sys.argv) != 3:
            print(f"python3 {sys.argv[0]} peer ID")
            exit()
        peer_process(int(sys.argv[2]))
    else:
        print(
            f"usage: python3 {sys.argv[0]} {{local N|comparison_server|name_server|peer ID}}"
        )

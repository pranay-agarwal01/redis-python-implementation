import secrets  
import string
import socket
import threading
import time
import argparse
import os
from .rdb_parser import RDBParser

def create_random_alphanumeric_string(length: int) -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(length))

class ResponseParser:
    def respSimpleString(message):
        return f"+{message}\r\n"

    def respBulkString(message):
        if message is None:
            return "$-1\r\n"
        return f"${len(message)}\r\n{message}\r\n"

    def respArray(messages: list):
        if not messages:
            return "*0\r\n"
        return (
            f"*{len(messages)}"
            + "\r\n"
            + "".join([ResponseParser.respBulkString(message) for message in messages])
        )


class RedisServer:
    def __init__(self):
        self.db_data: dict[str, list[str]] = {}
        self.dir = None
        self.dbfilename = None
        self.replicaof: tuple[str, int] | None = None
        self.master_replid = create_random_alphanumeric_string(40)
        self.master_repl_offset = 0
        self.role = "master"


    def store_key_value(self, key, value, expiry_time=None):
        # creating {key: [value, expiry]}
        self.db_data[key] = [value, expiry_time]

    def command_parser(self, command: str) -> str:
        all_tokens = command.split("\r\n")
        # Sample Commands
        # *1\r\n$4\r\nPING\r\n
        # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
        # *3\r\n$3\r\nSET\r\n$3\r\nhey\r\n$3\r\nyou\r\n
        # *5\r\n$3\r\nSET\r\n$9\r\npineapple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n
        # *2\r\n$3\r\nGET\r\n$3\r\nhey\r\n
        # print("DEBUG: Command Tokens", all_tokens)
        
        if all_tokens[0] == "*1" and all_tokens[1] == "$4" and all_tokens[2] == "PING":
            return ResponseParser.respSimpleString("PONG")
        
        elif (
            all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "ECHO"
        ):
            return ResponseParser.respBulkString(all_tokens[4])
        
        elif (
            all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "KEYS"
        ):
            keys = list(self.db_data.keys())
            return ResponseParser.respArray(keys)
        
        elif (
            (all_tokens[0] == "*3" or all_tokens[0] == "*5")
            and all_tokens[1] == "$3"
            and all_tokens[2] == "SET"
        ):
            expiry = None
            if len(all_tokens) > 8 and all_tokens[8] == "px":
                expiry = int(time.time() * 1000) + int(all_tokens[10])
            self.store_key_value(all_tokens[4], all_tokens[6], expiry)
            return ResponseParser.respSimpleString("OK")
        
        elif all_tokens[0] == "*2" and all_tokens[1] == "$3" and all_tokens[2] == "GET":
            key = all_tokens[4]
            if key not in self.db_data:
                return ResponseParser.respBulkString(None)
            value_list = self.db_data[key]
            is_expired = value_list[1] and value_list[1] < int(time.time() * 1000)
            if is_expired:
                return ResponseParser.respBulkString(None)
            return ResponseParser.respBulkString(value_list[0])
        
        elif (
            all_tokens[0] == "*3"
            and all_tokens[1] == "$6"
            and all_tokens[2] == "CONFIG"
        ):
            if all_tokens[4] == "GET":
                if all_tokens[6] == "dbfilename":
                    return ResponseParser.respArray(["dbfilename", self.dbfilename])
                if all_tokens[6] == "dir":
                    return ResponseParser.respArray(["dir", self.dir])
            return ResponseParser.respBulkString(None)

        elif all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "INFO":
            if self.replicaof is not None:
                return ResponseParser.respBulkString(f"role:{self.role}")
            return ResponseParser.respBulkString(f"role:{self.role}:master_replid:{self.master_replid}:master_repl_offset:{self.master_repl_offset}")
        
        else:
            return ResponseParser.respBulkString(None)

    def connect(self, connection: socket.socket) -> None:
        with connection:
            connected: bool = True
            while connected:
                command: str = connection.recv(1024).decode()
                print(f"DEBUG: recieved - {command.encode()}")
                connected = bool(command)
                response: str = self.command_parser(command)

                print(f"DEBUG: returning - {response.encode()}")
                connection.send(response.encode())

    def perform_handshake(self, host, port):
        master_socket = socket.create_connection((host, port))
        master_socket.send(ResponseParser.respArray(["PING"]).encode())

    def parse_args(self):
        parser = argparse.ArgumentParser(description="Redis Server")
        parser.add_argument("--dir", type=str, help="Directory to store data in")
        parser.add_argument("--dbfilename", type=str, help="File to store data in")
        parser.add_argument("--port", type=int, default=6379, help="Port to run server on")
        parser.add_argument("--replicaof", type=str, help="Replica of host:port")
        return parser.parse_args()

    def run(self):
        args = self.parse_args()
        print(f"Running redis server on port {args.port}")
 
        if args.dir is not None:
            self.dir = args.dir
        if args.dbfilename is not None:
            self.dbfilename = args.dbfilename

        if args.replicaof is not None:
            self.role = "slave"
            self.replicaof = args.replicaof.split(" ")
            self.perform_handshake(self.replicaof[0], int(self.replicaof[1]))

        if self.dir is not None and self.dbfilename is not None:
            rdb_file_path = os.path.join(self.dir, self.dbfilename)
            rdb_parser = RDBParser(
                rdb_file_path,
                set_key_value_callback=self.store_key_value,
            )
            rdb_parser.parse_rdb_file()

        server_socket = socket.create_server(("localhost", args.port), reuse_port=True)

        while True:
            conn, addr = server_socket.accept()  # wait for client
            thread = threading.Thread(target=self.connect, args=[conn])
            thread.start()


def main():
    server = RedisServer()
    server.run()


if __name__ == "__main__":
    main()


# hexdump -C dump.rdb

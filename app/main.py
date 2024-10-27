import secrets
import string
import socket
import threading
import time
import argparse
import os
import re
from .rdb_parser import RDBParser
from .resp import ResponseParser

all_replica_connection: list[socket.socket] = []


def create_random_alphanumeric_string(length: int) -> str:
    return "".join(
        secrets.choice(string.ascii_letters + string.digits) for _ in range(length)
    )


class ResponseParser:
    def respSimpleString(message):
        return f"+{message}\r\n"

    def respBulkString(message):
        if message is None:
            return "$-1\r\n"
        return f"${len(message)}\r\n{message}\r\n"

    def respRDBContent(message: bytes):
        return f"${len(message)}\r\n".encode() + message

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
        self.connection: socket.socket | None = None

    def add_connection_master_replication_list(self, connection):
        all_replica_connection.append(connection)

    def replica_propogation_for_write_commands(self, command: str):
        print(
            f"REPLICATING: {command.encode()} to total {len(all_replica_connection)} no.of connections."
        )
        for conn in all_replica_connection:
            conn.send(command.encode())

    def send_data(self, message, connection: socket.socket = None):
        if isinstance(message, str):
            message = message.encode()
        print("DEBUG[returning]:", message)
        return connection.send(message)

    def open_rdb_file(self):
        rdb_file_path = "./app/dump.rdb"
        if os.path.exists(rdb_file_path):
            with open(rdb_file_path, "rb") as f:
                return f.read()
        else:
            print("RDB file not found, returning hardcoded rdb content")
            return b"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"

    def store_key_value(self, key, value, expiry_time=None):
        # creating {key: [value, expiry]}
        self.db_data[key] = [value, expiry_time]

    def command_parser(self, command: str, connection: socket.socket) -> str:
        all_tokens = command.split("\r\n")
        # Sample Commands
        # *1\r\n$4\r\nPING\r\n
        # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
        # *3\r\n$3\r\nSET\r\n$3\r\nhey\r\n$3\r\nyou\r\n
        # *5\r\n$3\r\nSET\r\n$9\r\npineapple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n
        # *2\r\n$3\r\nGET\r\n$3\r\nhey\r\n
        # print("DEBUG: Command Tokens", all_tokens)

        if all_tokens[0] == "*1" and all_tokens[1] == "$4" and all_tokens[2] == "PING":
            self.send_data(ResponseParser.respSimpleString("PONG"), connection)

        elif (
            all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "ECHO"
        ):
            self.send_data(ResponseParser.respBulkString(all_tokens[4]), connection)

        elif (
            all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "KEYS"
        ):
            keys = list(self.db_data.keys())
            self.send_data(ResponseParser.respArray(keys), connection)

        elif (
            (all_tokens[0] == "*3" or all_tokens[0] == "*5")
            and all_tokens[1] == "$3"
            and all_tokens[2] == "SET"
        ):
            expiry = None
            if len(all_tokens) > 8 and all_tokens[8] == "px":
                expiry = int(time.time() * 1000) + int(all_tokens[10])
            self.store_key_value(all_tokens[4], all_tokens[6], expiry)
            self.replica_propogation_for_write_commands(command)
            if self.role == "master":
                self.send_data(ResponseParser.respSimpleString("OK"), connection)

        elif all_tokens[0] == "*2" and all_tokens[1] == "$3" and all_tokens[2] == "GET":
            key = all_tokens[4]
            if key not in self.db_data:
                self.send_data(ResponseParser.respBulkString(None), connection)
            value_list = self.db_data[key]
            is_expired = value_list[1] and value_list[1] < int(time.time() * 1000)
            if is_expired:
                self.send_data(ResponseParser.respBulkString(None), connection)
            else:
                self.send_data(ResponseParser.respBulkString(value_list[0]), connection)

        elif (
            all_tokens[0] == "*3"
            and all_tokens[1] == "$6"
            and all_tokens[2] == "CONFIG"
        ):
            if all_tokens[4] == "GET":
                if all_tokens[6] == "dbfilename":
                    self.send_data(
                        ResponseParser.respArray(["dbfilename", self.dbfilename]),
                        connection,
                    )
                if all_tokens[6] == "dir":
                    self.send_data(
                        ResponseParser.respArray(["dir", self.dir]), connection
                    )
            else:
                self.send_data(ResponseParser.respBulkString(None), connection)

        elif (
            all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "INFO"
        ):
            if self.replicaof is not None:
                self.send_data(
                    ResponseParser.respBulkString(f"role:{self.role}"), connection
                )
            else:
                self.send_data(
                    ResponseParser.respBulkString(
                        f"role:{self.role}:master_replid:{self.master_replid}:master_repl_offset:{self.master_repl_offset}"
                    ),
                    connection,
                )

        elif (
            all_tokens[0] == "*3"
            and all_tokens[1] == "$8"
            and all_tokens[2] == "REPLCONF"
        ):
            self.send_data(ResponseParser.respSimpleString("OK"), connection)

        elif (
            all_tokens[0] == "*3" and all_tokens[1] == "$5" and all_tokens[2] == "PSYNC"
        ):
            master_id = all_tokens[4]
            replication_offset = all_tokens[6]
            if master_id == "?" and replication_offset == "-1":
                self.send_data(
                    ResponseParser.respSimpleString(
                        f"FULLRESYNC {self.master_replid} {self.master_repl_offset}"
                    ),
                    connection,
                )
                rdb_content = self.open_rdb_file()
                self.send_data(ResponseParser.respRDBContent(rdb_content), connection)
                self.add_connection_master_replication_list(self.connection)
            else:
                self.send_data(ResponseParser.respBulkString(None), connection)

    def connect(self, connection: socket.socket) -> None:
        with connection:
            self.connection = connection
            connected: bool = True
            while connected:
                commands: str = connection.recv(1024).decode()
                print(f"DEBUG[connection] - {connection}")
                print(f"DEBUG: recieved - {commands.encode()}")
                connected = bool(commands)
                if commands:
                    # when getting replica propagation command, we can get all command at once, hence splitting the command with '*'
                    # example: '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'
                    all_commands = re.split(r"(?=\*)", commands)[1:]
                    for command in all_commands:
                        if command:
                            self.command_parser(command, connection)

    def perform_handshake(self, host, port, port_to_listen_on):
        master_socket = socket.create_connection((host, port))
        print(f"MASTER_CONNECTION - {master_socket}")
        master_socket.send(ResponseParser.respArray(["PING"]).encode())
        master_socket.recv(1024)
        master_socket.send(
            ResponseParser.respArray(
                ["REPLCONF", "listening-port", str(port_to_listen_on)]
            ).encode()
        )
        master_socket.recv(1024)
        master_socket.send(
            ResponseParser.respArray(["REPLCONF", "capa", "psync2"]).encode()
        )
        master_socket.recv(1024)
        master_socket.send(ResponseParser.respArray(["PSYNC", "?", "-1"]).encode())
        master_socket.recv(1024)
        master_socket.send(ResponseParser.respArray(["REPLCONF", "ACK", "0"]).encode())
        thread = threading.Thread(target=self.connect, args=[master_socket])
        thread.start()

    def parse_args(self):
        parser = argparse.ArgumentParser(description="Redis Server")
        parser.add_argument("--dir", type=str, help="Directory to store data in")
        parser.add_argument("--dbfilename", type=str, help="File to store data in")
        parser.add_argument(
            "--port", type=int, default=6379, help="Port to run server on"
        )
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
            self.perform_handshake(self.replicaof[0], int(self.replicaof[1]), args.port)

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
            print(f"MAIN_CONNECTION - {conn}")
            thread = threading.Thread(target=self.connect, args=[conn])
            thread.start()


def main():
    server = RedisServer()
    server.run()


if __name__ == "__main__":
    main()

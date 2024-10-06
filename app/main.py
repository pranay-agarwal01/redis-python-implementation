import socket
import threading
import time
import argparse
import os

class ResponseParser:
    def respSimpleString(message):
        return f"+{message}\r\n"
    
    def respBulkString(message):
        if message is None:
            return "$-1\r\n"
        return f"${len(message)}\r\n{message}\r\n"
    
    def respArray(messages:list):
        if not messages:
            return "*0\r\n"
        return f"*{len(messages)}" + "\r\n" + "".join([ResponseParser.respBulkString(message) for message in messages])
    
class RedisServer:
    def __init__(self):
        self.db_data = {}
        self.dir = None
        self.dbfilename = None

    def parse_redis_file_format(self, rbd_content: str):
        splited_parts = rbd_content.split("\\")
        print("DEBUG: Splited Parts", splited_parts)
        resizedb_index = splited_parts.index("xfb")
        key_index = resizedb_index + 4
        key_bytes = splited_parts[key_index]
        key = self.remove_bytes_caracteres(key_bytes)
        return key

    def remove_bytes_caracteres(self, string: str):
        if string.startswith("x"):
            return string[3:]
        elif string.startswith("t"):
            return string[1:]

    def read_db_file(self):
        rdb_file_path = os.path.join(self.dir, self.dbfilename)
        if os.path.exists(rdb_file_path):
            with open(rdb_file_path, "rb") as f:
                rbd_content = str(f.read())
                if rbd_content:
                    return self.parse_redis_file_format(rbd_content)
        return None

    def command_parser(self, command: str) -> str:
        all_tokens = command.split("\r\n")
        #Sample Commands
        # *1\r\n$4\r\nPING\r\n
        # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n 
        # *3\r\n$3\r\nSET\r\n$3\r\nhey\r\n$3\r\nyou\r\n
        # *5\r\n$3\r\nSET\r\n$9\r\npineapple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n
        # *2\r\n$3\r\nGET\r\n$3\r\nhey\r\n
        print("DEBUG: Command Tokens", all_tokens)
        if all_tokens[0] == "*1" and all_tokens[1] == "$4" and all_tokens[2] == "PING":
            return ResponseParser.respSimpleString("PONG")
        elif all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "ECHO":
            return ResponseParser.respBulkString(all_tokens[4])
        elif all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "KEYS":
            key = self.read_db_file()
            return ResponseParser.respArray([key])
        elif (all_tokens[0] == "*3" or all_tokens[0] == "*5") and all_tokens[1] == "$3" and all_tokens[2] == "SET":
            # creating a list [value, expiry]
            expiry = None
            if len(all_tokens) > 8 and all_tokens[8] == "px":
                expiry = int(time.time() * 1000) + int(all_tokens[10])
            self.db_data[all_tokens[4]] = [all_tokens[6], expiry]
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
        elif all_tokens[0] == "*3" and all_tokens[1] == "$6" and all_tokens[2] == "CONFIG":
            if all_tokens[4] == "GET":
                if all_tokens[6] == "dbfilename":
                    return ResponseParser.respArray(["dbfilename",self.dbfilename])
                if all_tokens[6] == "dir":
                    return ResponseParser.respArray(["dir",self.dir])
            return ResponseParser.respBulkString(None)
        else:
            return ResponseParser.respBulkString(None)

    def connect(self, connection: socket.socket) -> None:
        with connection:
            connected: bool = True
            while connected:
                command:str = connection.recv(1024).decode()
                print(f"DEBUG: recieved - {command.encode()}")
                connected = bool(command)
                response: str  = self.command_parser(command)
                
                print(f"DEBUG: returning - {response.encode()}")
                connection.send(response.encode())

    def parse_args(self):
        parser = argparse.ArgumentParser(description="Redis Server")
        parser.add_argument('--dir', type=str, help='Directory to store data in')
        parser.add_argument('--dbfilename', type=str, help='File to store data in')
        return parser.parse_args()

    def run(self):
        args = self.parse_args()
        print("Running redis server on port 6379")

        if args.dir is not None:
            self.dir = args.dir
        if args.dbfilename is not None:
            self.dbfilename = args.dbfilename

        server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

        while True:
            conn, addr = server_socket.accept() # wait for client
            thread = threading.Thread(target=self.connect, args=[conn])
            thread.start()

def main():
    server = RedisServer()
    server.run()

if __name__ == "__main__":
    main()


# hexdump -C dump.rdb
import socket   
import threading
import time

class ResponseParser:
    def respSimpleString(message):
        return f"+{message}\r\n"
    
    def respBulkString(message):
        if message is None:
            return "$-1\r\n"
        return f"${len(message)}\r\n{message}\r\n"
    
pong = "+PONG\r\n"
db_data = {}

def command_parser(command: str) -> str:
    all_tokens = command.split("\r\n")
    #Sample Commands
    # *1\r\n$4\r\nPING\r\n
    # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n 
    # *3\r\n$3\r\nSET\r\n$3\r\nhey\r\n$3\r\nyou\r\n
    # *5\r\n$3\r\nSET\r\n$9\r\npineapple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n
    # *2\r\n$3\r\nGET\r\n$3\r\nhey\r\n
    print("DEBUG: Command Tokens", all_tokens)
    if all_tokens[0] == "*1" and all_tokens[1] == "$4" and all_tokens[2] == "PING":
        return ResponseParser.respBulkString(pong)
    elif all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "ECHO":
        return ResponseParser.respBulkString(all_tokens[4])
    elif (all_tokens[0] == "*3" or all_tokens[0] == "*5") and all_tokens[1] == "$3" and all_tokens[2] == "SET":
        # creating a list [value, expiry]
        expiry = None
        if len(all_tokens) > 8 and all_tokens[8] == "px":
            expiry = int(time.time() * 1000) + int(all_tokens[10])
        db_data[all_tokens[4]] = [all_tokens[6], expiry]
        return ResponseParser.respSimpleString("OK")
    elif all_tokens[0] == "*2" and all_tokens[1] == "$3" and all_tokens[2] == "GET":
        key = all_tokens[4]
        if key not in db_data:
            return ResponseParser.respBulkString(None)
        value_list = db_data[key]
        is_expired = value_list[1] and value_list[1] < int(time.time() * 1000)
        if is_expired:
            return ResponseParser.respBulkString(None)
        return ResponseParser.respBulkString(value_list[0])
    else:
        return ResponseParser.respBulkString(None)

def connect(connection: socket.socket) -> None:
    with connection:
        connected: bool = True
        while connected:
            command:str = connection.recv(1024).decode()
            print(f"DEBUG: recieved - {command.encode()}")
            connected = bool(command)
            response: str  = command_parser(command)
                
            print(f"DEBUG: returning - {response.encode()}")
            connection.send(response.encode())

def main():
    print("Running redis server on port 6379")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        conn, addr = server_socket.accept() # wait for client
        thread = threading.Thread(target=connect, args=[conn])
        thread.start()

if __name__ == "__main__":
    main()

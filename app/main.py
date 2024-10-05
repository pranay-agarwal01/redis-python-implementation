import socket   
import threading

db_data = {}

def command_parser(command: str) -> str:
    all_tokens = command.split("\r\n")
    #Sample Commands
    # *1\r\n$4\r\nPING\r\n
    # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n 
    # *3\r\n$3\r\nSET\r\n$3\r\nhey\r\n$3\r\nyou\r\n
    # *2\r\n$3\r\nGET\r\n$3\r\nhey\r\n
    print("DEBUG: Command Tokens", all_tokens)
    if all_tokens[0] == "*1" and all_tokens[1] == "$4" and all_tokens[2] == "PING":
        return "+PONG\r\n"
    elif all_tokens[0] == "*2" and all_tokens[1] == "$4" and all_tokens[2] == "ECHO":
        return f"${len(all_tokens[4])}\r\n{all_tokens[4]}\r\n"
    elif all_tokens[0] == "*3" and all_tokens[1] == "$3" and all_tokens[2] == "SET":
        db_data[all_tokens[4]] = all_tokens[6]
        return "+OK\r\n"
    elif all_tokens[0] == "*2" and all_tokens[1] == "$3" and all_tokens[2] == "GET":
        key = all_tokens[4]
        if key not in db_data:
            return "$-1\r\n"
        return f"${len(db_data[key])}\r\n{db_data[key]}\r\n"
    else:
        return "$-1\r\n"

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

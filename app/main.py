import socket  # noqa: F401
import threading

def connect(connection: socket.socket) -> None:
    with connection:
        connected: bool = True
        while connected:
            command:str = connection.recv(1024).decode()
            print(f"DEBUG: recieved - {command.encode()}")
            connected = bool(command)
            response: str  = "+PONG\r\n"
            if "ECHO" in command:
                res_data = command.split("\r\n")[-2]
                content_len = len(res_data)
                response = f"${content_len}\r\n{res_data}\r\n"
                
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

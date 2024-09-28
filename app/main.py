import socket  # noqa: F401
import threading

def on_new_client(conn,addr):
    while True:
        msg = conn.recv(1024)
        #do some checks and if msg == someWeirdSignal: break:
        print("Received: ", addr, " >> ", msg)
        #Maybe some code to compute the last digit of PI, play game or anything else can go here and when you are done.
        conn.send("+PONG\r\n".encode())


def main():
    print("Running redis server on port 6379")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        conn, addr = server_socket.accept() # wait for client
        thread = threading.Thread(target=on_new_client, args=(conn,addr))
        thread.start()


if __name__ == "__main__":
    main()

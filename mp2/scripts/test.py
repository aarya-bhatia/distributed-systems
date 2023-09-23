from sys import stdin
import socket

hosts = {}

def print_usage():
    print(f"<cmd> <target> <val>")

def load_server():
    with open("hosts", "r") as f:
        lines = f.readlines()
        for line in lines:
            words = line.split(" ")
            hosts[words[0]] = {"host_name": words[1], "port": words[2]}
    print(hosts)
    print()
def main():
    load_server()
    for line in stdin:
        words = line.split(" ")
        if len(words) < 2:
            print_usage()
            continue
        cmd = words[0]
        target = words[1]
        if target == 'all':
            target = range(1, 11)
        else:
            target = [int(words[1])]

        if cmd == "droprate":
            for host in target:
                host = str(host)
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                message = f"CONFIG DROPRATE {words[2]}"
                print(f"sending {message}")
                # sock.sendto(bytes(message, "utf-8"), (hosts[host]["host_name"], int(hosts[host]["port"])))
                sock.sendto(bytes("id", "utf-8"), (hosts[host]["host_name"], int(hosts[host]["port"])))
                data, server = sock.recvfrom(1024)
                print(str(data))


if __name__ == '__main__':
    main()
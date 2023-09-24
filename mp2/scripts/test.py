from sys import stdin
import socket

hosts = {}

def print_usage():
    print(f"set <target>")

def load_server():
    with open("hosts", "r") as f:
        lines = f.readlines()
        for line in lines:
            words = line.split(" ")
            hosts[words[0]] = {"host_name": words[1], "port": words[2]}
    print(hosts)

def main():
    load_server()
    for line in stdin:
        line = line.strip()
        words = line.split(";")
        if len(words) < 2:
            print_usage()
            continue
        cmd = words[0]
        target = words[1]
        if target == 'all':
            target = range(1, 11)
        else:
            vms = words[1].split(',')
            target = []
            for vm in vms:
                target.append(vm)
        print(f"sending {cmd} to {target}")
        for host in target:
            host = str(host)
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(0.2)
            
            host_name = hosts[host]["host_name"]
            host_port = int(hosts[host]["port"])
            sock.sendto(bytes(cmd, "utf-8"), (host_name, host_port))
            print("-------")
            try:
                data, server = sock.recvfrom(1024)
                print(f'{host_name}:{host_port} {data.decode()}')
            except socket.timeout:
                print(f'{host_name}:{host_port} No response in 0.2 seconds')


if __name__ == '__main__':
    main()
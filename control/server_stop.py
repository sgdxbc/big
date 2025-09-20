from common import *
from time import sleep


def task(hosts):
    join([Ssh(host, "pkill -INT big") for host in hosts], allow_fail=True)


if __name__ == "__main__":
    import clusters

    server_hosts = [item["host"] for item in clusters.server]
    task(server_hosts)

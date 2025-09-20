from common import *


def task(hosts):
    join([Ssh(host, "pkill big") for host in hosts], allow_fail=True)
    join([Ssh(host, "rm -r /tmp/big*") for host in hosts], allow_fail=True)


if __name__ == "__main__":
    import clusters

    hosts = [item["host"] for item in clusters.client + clusters.server]
    task(hosts)

from common import *


def task(hosts):
    wait_all([Ssh(host, "pkill big") for host in hosts], fail_ok=True)
    wait_all([Ssh(host, "rm -r /tmp/big*") for host in hosts], fail_ok=True)


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.client + clusters.server])

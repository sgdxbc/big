from common import *
from time import sleep


def task(hosts):
    wait_all([Ssh(host, "pkill -INT big") for host in hosts], fail_ok=True)


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.server])

from common import *
from time import sleep


def task(hosts):
    for host in hosts:
        try:
            ssh(host, f"pkill -INT big")
        except RuntimeError:
            pass


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.server])

from common import *
import load_config
import server_start
import server_stop
import download_logs


def task(server_hosts):
    load_config.task(server_hosts)
    tasks = server_start.task(server_hosts, "bench")
    try:
        wait_all(tasks)
    finally:
        server_stop.task(server_hosts)
    download_logs.task(server_hosts, "big.log")


if __name__ == "__main__":
    import clusters

    server_hosts = [item["host"] for item in clusters.server]
    task(server_hosts)

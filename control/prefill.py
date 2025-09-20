from common import *
import load_config
import server_start
import download_logs


def task(server_hosts):
    load_config.task(server_hosts)
    tasks = server_start.tasks(server_hosts, "prefill")
    try:
        join(tasks)
    finally:
        download_logs.task(server_hosts, "big.log")


if __name__ == "__main__":
    import clusters

    server_hosts = [item["host"] for item in clusters.server]
    task(server_hosts)

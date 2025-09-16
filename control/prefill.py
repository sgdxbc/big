from common import *
import load_config
import server_start
import server_stop
import download_logs


def task(server_hosts):
    load_config.task(server_hosts)
    tasks = server_start.task(server_hosts, "prefill")
    try:
        wait_all(tasks)
    except:
        server_stop.task(server_hosts)
        raise
    download_logs.task(server_hosts, "big.log")


if __name__ == "__main__":
    import clusters

    server_hosts = [item["host"] for item in clusters.server]
    task(server_hosts)

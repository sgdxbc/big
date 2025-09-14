from common import *
import load_config
import server_start
import server_stop
import download_logs


def task(server_hosts, num_faulty_node, plain):
    if plain:
        assert num_faulty_node == 0
    num_node = num_faulty_node * 3 + 1
    assert len(server_hosts) >= num_node
    with open("configs/task.override.conf", "w") as f:
        f.write(f"big.num-node {num_node}\n")
        f.write(f"big.num-faulty-node {num_faulty_node}\n")
        f.write(f"big.plain-storage {str(plain).lower()}\n")
    load_config.task(server_hosts[:num_node])
    tasks = server_start.task(server_hosts[:num_node], "bench")
    try:
        wait_all(tasks)
    finally:
        server_stop.task(server_hosts[:num_node])
    download_logs.task(server_hosts[:num_node], "big.log")


if __name__ == "__main__":
    import clusters
    import sys

    argv = dict(enumerate(sys.argv))
    server_hosts = [item["host"] for item in clusters.server]
    if argv.get(1) == "plain":
        task(server_hosts, 0, True)
    else:
        task(server_hosts, int(argv.get(1, "0")), False)

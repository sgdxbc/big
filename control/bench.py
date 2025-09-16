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
    except:
        server_stop.task(server_hosts)
        raise
    finally:
        download_logs.task(server_hosts, "big.log")


def override_task(server_hosts, num_faulty_node, plain):
    if plain:
        assert num_faulty_node == 0
    num_node = num_faulty_node * 3 + 1
    assert len(server_hosts) >= num_node
    with open("configs/task.override.conf", "w") as f:
        f.write(f"big.num-node {num_node}\n")
        f.write(f"big.num-faulty-node {num_faulty_node}\n")
        f.write(f"big.plain-storage {str(plain).lower()}\n")


if __name__ == "__main__":
    import clusters

    server_hosts = [item["host"] for item in clusters.server]
    task(server_hosts)

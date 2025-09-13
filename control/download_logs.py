from common import *


def task(hosts, log_path):
    # local("rm -r logs; mkdir logs")
    local("mkdir -p logs")
    with open("logs/.gitignore", "w") as f:
        f.write("*")
    wait_all([
        Local(f"rsync {host}:{log_path} logs/{host}.log", host=host) for host in hosts
    ], fail_ok=True)


if __name__ == "__main__":
    import clusters
    from sys import argv

    task([item["host"] for item in clusters.client + clusters.server], argv[1])

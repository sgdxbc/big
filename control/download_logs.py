from common import *


def task(hosts, log_path):
    # local("rm -r logs; mkdir logs")
    Local("mkdir -p logs").wait()
    with open("logs/.gitignore", "w") as f:
        f.write("*")
    join(
        [Local(f"rsync {host}:{log_path} logs/{host}.log") for host in hosts],
        allow_fail=True,
    )


if __name__ == "__main__":
    import clusters
    from sys import argv

    hosts = [item["host"] for item in clusters.client + clusters.server]
    task(hosts, argv[1])

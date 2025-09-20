from common import *


def task(hosts):
    join(
        [
            Local(f"rsync -a configs/*.conf {host}:{deploy_dir}/big-configs/")
            for host in hosts
        ]
    )


if __name__ == "__main__":
    import clusters

    hosts = [item["host"] for item in clusters.server + clusters.client]
    task(hosts)

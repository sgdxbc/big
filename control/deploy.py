from common import *
from concurrent.futures import *
import remote_build
import clusters


def task(build_host, sync_hosts):
    remote_build.task(build_host)
    ssh(build_host, f"cp {build_dir}/target/release/big {deploy_dir}/")
    if not nfs:
        for host in sync_hosts:
            ssh(build_host, f"rsync -a {deploy_dir}/big {host}:{deploy_dir}/")


if __name__ == "__main__":
    task(
        clusters.client[0]["host"],
        [item["host"] for item in clusters.client[1:] + clusters.server],
    )

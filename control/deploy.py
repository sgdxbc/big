from common import *
from concurrent.futures import *
import remote_build
import clusters


def task(build_host, sync_hosts):
    remote_build.task(build_host)
    Ssh(build_host, f"cp {build_dir}/target/release/big {deploy_dir}/").wait()
    for host in sync_hosts:
        Ssh(build_host, f"rsync -a {deploy_dir}/big {host}:{deploy_dir}/").wait()


if __name__ == "__main__":
    build_host = clusters.client[0]["host"]
    sync_hosts = [item["host"] for item in clusters.client[1:] + clusters.server]
    task(build_host, sync_hosts)

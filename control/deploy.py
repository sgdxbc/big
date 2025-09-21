from common import *
from concurrent.futures import *
from time import sleep
import remote_build
import clusters


def task(build_host, build_ip, sync_hosts):
    remote_build.task(build_host)
    Ssh(build_host, f"cp {build_dir}/target/release/big {deploy_dir}/").wait()
    http_server = Ssh(build_host, f"python3 -m http.server -d {deploy_dir}")
    sleep(1)
    join(
        [
            Ssh(host, f"curl -s {build_ip}:8000/big -o {deploy_dir}/big")
            for host in sync_hosts
        ]
    )
    join([Ssh(host, f"chmod +x {deploy_dir}/big") for host in sync_hosts])
    Ssh(build_host, "pkill -INT -f http.server").wait()
    http_server.wait()


if __name__ == "__main__":
    build_instance = clusters.client[0]
    sync_hosts = [item["host"] for item in clusters.client[1:] + clusters.server]
    task(build_instance["host"], build_instance["ip"], sync_hosts)

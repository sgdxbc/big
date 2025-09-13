from common import *
from concurrent.futures import *
import clusters


def build_task(build_host):
    ssh(
        build_host,
        "/bin/bash -l -c 'which cargo' || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y",
    )
    ssh(build_host, "/bin/bash -l -c 'which cc' || (sudo apt-get update && sudo apt-get install -y clang)")
    if login_key:
        local(f"rsync -a {login_key} {build_host}:.ssh/id_ed25519")
        write_file(
            build_host,
            ".ssh/config",
            """
Host *.compute.amazonaws.com
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
    LogLevel Quiet
""",
        )


def task(hosts):
    addr_conf = "\n".join(
        f"addrs {item['ip']}:{service_port}" for item in clusters.server
    )

    for host in hosts:
        ssh(host, f"mkdir -p {deploy_dir}/big-configs")
        write_file(host, f"{deploy_dir}/big-configs/addr.conf", addr_conf)
        if nfs:
            break


if __name__ == "__main__":
    build_task(clusters.client[0]["host"])
    task([item["host"] for item in clusters.server + clusters.client])

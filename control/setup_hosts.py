from common import *
from concurrent.futures import *
import clusters


def build_task(build_host):
    try:
        Ssh(
            build_host,
            "/bin/bash -l -c 'which cargo2'"
        ).wait()
    except:
        Ssh(
            build_host,
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y"
        ).wait()
        Ssh(
            build_host,
            "curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash -l"
        ).wait()
        Ssh(
            build_host,
            "bash -l -c 'cargo binstall -y cargo-sweep'"
        ).wait()
    Ssh(
        build_host,
        "/bin/bash -l -c 'which cc' || (sudo apt-get update && sudo apt-get install -y clang make)",
    ).wait()
    if login_key:
        Local(f"rsync -a {login_key} {build_host}:.ssh/id_ed25519").wait()
        WriteRemote(
            build_host,
            ".ssh/config",
            """
Host *.compute.amazonaws.com
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
    LogLevel Quiet
""",
        ).wait()


def storage_task(hosts):
    for host in hosts:
        try:
            Ssh(host, "mount | grep /tmp").wait()
        except:
            Ssh(host, "sudo mkfs.xfs -f /dev/nvme1n1").wait()
            Ssh(host, "sudo mount -o discard /dev/nvme1n1 /tmp").wait()
            Ssh(host, "sudo chmod 777 /tmp").wait()


def common_task(hosts):
    addr_conf = "\n".join(
        f"addrs {item['ip']}:{service_port}" for item in clusters.server
    )

    join([Ssh(host, f"mkdir -p {deploy_dir}/big-configs") for host in hosts])
    join(
        [
            WriteRemote(host, f"{deploy_dir}/big-configs/addr.conf", addr_conf)
            for host in hosts
        ]
    )


if __name__ == "__main__":
    client_hosts = [item["host"] for item in clusters.client]
    server_hosts = [item["host"] for item in clusters.server]
    build_task(client_hosts[0])
    storage_task(server_hosts)
    common_task(server_hosts + client_hosts)

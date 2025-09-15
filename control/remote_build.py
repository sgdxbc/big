from common import *


def task(host, example=None):
    if not example:
        build_flag = "--bin big"
    else:
        build_flag = f"--example {example}"
    local(f"cargo build -r {build_flag}")  # sanity check
    ssh(host, f"mkdir -p {build_dir}")
    local(f"rsync -aR src/ examples/ Cargo.toml Cargo.lock {host}:{build_dir}/")
    ssh(host, f"cd {build_dir} && /bin/bash -l -c 'cargo build -r {build_flag}'")


if __name__ == "__main__":
    import clusters
    import sys

    argv = dict(enumerate(sys.argv))
    task(clusters.client[0]["host"], example=argv.get(1))

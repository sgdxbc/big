from common import *


def task(host, example=None):
    if not example:
        build_flag = "--bin big"
    else:
        build_flag = f"--example {example}"
    Local(f"cargo build -r {build_flag}").wait()  # sanity check
    Ssh(host, f"mkdir -p {build_dir}").wait()
    Local(f"rsync -aR --delete src/ examples/ Cargo.toml Cargo.lock {host}:{build_dir}/").wait()
    # Ssh(host, f"cd {build_dir} && /bin/bash -l -c 'cargo sweep --stamp'").wait()
    Ssh(host, f"cd {build_dir} && /bin/bash -l -c 'cargo build -r {build_flag}'").wait()
    # Ssh(host, f"cd {build_dir} && /bin/bash -l -c 'cargo sweep --file'").wait()


if __name__ == "__main__":
    import clusters
    import sys

    argv = dict(enumerate(sys.argv))
    task(clusters.client[0]["host"], example=argv.get(1))

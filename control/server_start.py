from common import *


def task(hosts, role):
    tasks = []
    for index, host in enumerate(hosts):
        tasks.append(Ssh(host, f"cd {deploy_dir} && ./big {role}.{index}"))
    return tasks


if __name__ == "__main__":
    import clusters

    task([item["host"] for item in clusters.server])

from subprocess import Popen, PIPE


service_port = 5000
build_dir = "/tmp/bftk"
deploy_dir = "/app"
nfs = False
terraform = False
login_key = None


class Local:
    def __init__(self, cmd, host=None):
        print(f"[local] {cmd}")
        self.host = host or "local"
        self.process = Popen(cmd, shell=True)

    def wait(self):
        if self.process.wait() != 0:
            raise RuntimeError()


class Ssh:
    def __init__(self, host, cmd):
        print(f"[ssh {host}] {cmd}")
        self.host = host
        self.process = Popen(["ssh", host, cmd])

    def wait(self):
        if self.process.wait() != 0:
            raise RuntimeError()


def wait_all(tasks, fail_ok=False):
    num_fail = 0
    for task in tasks:
        try:
            task.wait()
        except RuntimeError:
            if not fail_ok:
                print(f"Task failed on {task.host}")
                raise
            else:
                num_fail += 1
    if num_fail:
        print(f"{num_fail} task(s) failed")


def local(cmd, detach=False):
    print(f"[local] {cmd}")
    process = Popen(cmd, shell=True)
    if detach:
        return process
    if process.wait() != 0:
        raise RuntimeError()


def ssh(host, cmd, detach=False):
    print(f"[ssh {host}] {cmd}")
    process = Popen(["ssh", host, cmd])
    if detach:
        return process
    if process.wait() != 0:
        raise RuntimeError()


def write_file(host, path, content):
    print(f"[write_file {host} {path}]")
    process = Popen(["ssh", host, "cat", ">", path], text=True, stdin=PIPE)
    process.communicate(input=content)
    if process.returncode != 0:
        raise RuntimeError()


try:
    from common_override import *
except ImportError:
    pass

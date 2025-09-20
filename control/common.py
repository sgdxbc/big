from subprocess import PIPE, TimeoutExpired
from psutil import Popen, wait_procs


service_port = 5000
build_dir = "/tmp/bftk"
deploy_dir = "/app"
nfs = False
terraform = False
login_key = None


class Task:
    def wait(self, allow_fail=False):
        return_code = self.process.wait()
        assert allow_fail or return_code == 0


class Local(Task):
    def __init__(self, cmd):
        print(f"[local] {cmd}")
        self.cmd = cmd
        self.process = Popen(cmd, shell=True)


class Ssh(Task):
    def __init__(self, host, cmd):
        print(f"[ssh {host}] {cmd}")
        self.host = host
        self.cmd = cmd
        self.process = Popen(["ssh", host, cmd])


class WriteRemote(Task):
    def __init__(self, host, path, content):
        print(f"[write_file {host} {path}]")
        self.host = host
        self.process = Popen(["ssh", host, "cat", ">", path], text=True, stdin=PIPE)
        try:
            self.process.communicate(input=content, timeout=0.1)
        except TimeoutExpired:
            pass


def join(tasks, allow_fail=False):
    def cb(p):
        if p.returncode != 0 and not allow_fail:
            print("task failed")
            num_terminated = 0
            for t in tasks:
                if t.process.is_running():
                    t.process.terminate()
                    num_terminated += 1
                elif t.process is p:
                    if isinstance(t, (Ssh, WriteRemote)):
                        print(f"  {t.host}")
                    if isinstance(t, (Local, Ssh)):
                        print(f"  {t.cmd}")
            print(f"terminated {num_terminated} other tasks")
            raise RuntimeError()

    gone, alive = wait_procs([task.process for task in tasks], callback=cb)
    assert not alive
    num_fail = len([p for p in gone if p.returncode != 0])
    if num_fail:
        print(f"{num_fail} task(s) failed")


try:
    from common_override import *
except ImportError:
    pass

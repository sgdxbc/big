from subprocess import run
from json import loads


def run_terraform(key):
    proc = run(
        f"terraform -chdir=control/terraform output -json {key}",
        shell=True,
        capture_output=True,
        text=True,
        check=True,
    )
    instances = loads(proc.stdout)
    return [
        {
            "host": instance["public_dns"],
            "ip": instance["private_ip"],
        }
        for instance in instances
    ]


server = run_terraform("server")
client = run_terraform("client")


if __name__ == "__main__":
    print("client")
    for instance in client:
        print(f"{instance['host']:<60}{instance['ip']}")
    print("server")
    for instance in server:
        print(f"{instance['host']:<60}{instance['ip']}")

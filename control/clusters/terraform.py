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
    from pprint import pprint as print

    print(server)
    print(client)

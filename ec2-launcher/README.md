# EC2 Launcher CLI

A comfortable `argparse` + `boto3` CLI that launches an **Amazon Linux 2023**
EC2 instance into a VPC/subnet you specify, locks SSH to your IP, and verifies
port 22 is reachable.

> There is no "Amazon Linux 3" — **AL2023** is the newest Amazon Linux line
> (it succeeds Amazon Linux 2), so the tool targets `al2023-ami-2023.*-x86_64`
> by default (override with `--ami-name-pattern`).

## What it does (`create`)

1. **Validates** the given `--vpc-id` and `--subnet-id` against AWS (and checks
   the subnet really belongs to the VPC).
2. **Resolves the latest AMI** dynamically via `describe_images` (owner
   `amazon`), picking the newest by creation date.
3. **Determines the SSH IP automatically** (your external IP, via
   `checkip.amazonaws.com`), or uses `--custom-ssh-ip` if provided.
4. Creates a **key pair** and writes `<key-name>.pem` with **`0400`**
   permissions (`os.chmod(path, 0o400)`).
5. Creates a **security group** allowing TCP 22 only from that IP/CIDR.
6. **Launches** the instance (with a public IP), waits for `running`, then
   **probes port 22 with a raw `socket`** until it opens.
7. Every AWS call is wrapped in **try/except** that prints the error `Code` and
   `Message` cleanly (e.g. `InvalidVpcID.NotFound`, `UnauthorizedOperation`).

## Usage

Credentials are read from the project `.env`
(`aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`,
`aws_region_name`).

```bash
# SSH IP auto-detected
python main.py create --vpc-id vpc-xxxx --subnet-id subnet-xxxx

# Override the allowed SSH IP / CIDR, pick instance type
python main.py create --vpc-id vpc-xxxx --subnet-id subnet-xxxx \
    --custom-ssh-ip 203.0.113.10 --instance-type t3.small

# Tear everything down again
python main.py destroy --instance-id i-xxxx --security-group-id sg-xxxx \
    --key-name ec2-launcher-key --pem-file ec2-launcher-key.pem
```

### `create` arguments

| Argument | Default | Purpose |
|----------|---------|---------|
| `--vpc-id` | (required) | VPC to launch into (validated) |
| `--subnet-id` | (required) | Subnet to launch into (validated, must be in the VPC) |
| `--instance-type` | `t3.micro` | EC2 instance type |
| `--key-name` | `ec2-launcher-key` | Key pair name; `.pem` saved as `<key-name>.pem` (0400) |
| `--sg-name` | `ec2-launcher-sg` | Security group name to create |
| `--name` | `ec2-launcher-instance` | Name tag for the instance |
| `--custom-ssh-ip` | auto-detected | SSH source IP or CIDR override |
| `--ami-name-pattern` | `al2023-ami-2023.*-x86_64` | AMI name filter |
| `--port-timeout` | `180` | Seconds to wait for port 22 |
| `--region` | from `.env` | AWS region (top-level flag) |

## Notes

- The instance gets a public IP (`AssociatePublicIpAddress`) so the socket probe
  can reach it — make sure the subnet routes to an Internet Gateway.
- `destroy` is provided for convenient cleanup so you don't leave billable
  resources running.

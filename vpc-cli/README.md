# VPC CLI

An `argparse`-based CLI tool that fully provisions an AWS VPC with **N public**
and **N private** subnets, or tears the whole stack back down.

It builds on the lecture VPC script and extends it: instead of a single
public/private subnet pair, you pick how many subnets of each tier to create
with the `-n` argument. The subnet CIDRs are carved automatically from the VPC
CIDR and the subnets are spread round-robin across the region's Availability
Zones.

## Public vs. private subnet

The difference is **routing**, not the subnet itself:

| | Public subnet | Private subnet |
|---|---|---|
| Route table | has `0.0.0.0/0 -> Internet Gateway` | **no** internet route |
| Auto-assign public IPv4 | enabled (`MapPublicIpOnLaunch`) | disabled |
| Reachable from the internet | yes | no (isolated) |

All public subnets share one public route table; all private subnets share one
private route table.

## Usage

```bash
# Create a VPC with 3 public + 3 private subnets
python script.py create --vpc-name my-vpc -n 3

# Custom VPC CIDR and fixed /24 subnets
python script.py create --vpc-name my-vpc -n 4 --vpc-cidr 172.16.0.0/16 --subnet-prefix 24

# Override region (defaults to aws_region_name in .env)
python script.py --region eu-west-1 create --vpc-name my-vpc -n 2

# Destroy everything by Name tag
python script.py destroy --vpc-name my-vpc
```

### Arguments

- `--region` — AWS region (defaults to `aws_region_name` from `.env`).
- `create`
  - `--vpc-name` (required) — Name tag for the VPC.
  - `--vpc-cidr` — VPC CIDR block (default `10.0.0.0/16`).
  - `-n`, `--num-subnets` — subnets **per tier** (creates N public + N private).
  - `--subnet-prefix` — optional fixed prefix length per subnet (e.g. `24`).
- `destroy`
  - `--vpc-name` (required) — VPC to delete, found by its Name tag.

## Subnet limit

A VPC has a default soft limit of **200 subnets**. Since the tool creates
`N public + N private = 2N` subnets, `-n` is capped at **100**. AWS does allow
more, but only after you open a support case and request an exclusive limit
increase.

Credentials are read from `.env` (`aws_access_key_id`, `aws_secret_access_key`,
`aws_session_token`, `aws_region_name`).

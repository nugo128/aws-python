# rds-launcher

A small `argparse` + `boto3` CLI that provisions a **publicly reachable MySQL
RDS instance** and verifies you can connect to it (DBeaver / DataGrip /
`mysql` CLI).

## What `create` does

1. **Discovers subnets** across at least two Availability Zones in the given
   VPC (RDS requires ≥ 2 AZs) and builds a **DB subnet group**.
2. Creates a **security group** that opens the MySQL port **3306 to any IP
   (`0.0.0.0/0`)** so external tools can connect.
3. Creates a **MySQL** instance with **60 GB** of storage, `PubliclyAccessible=True`.
4. Waits until the instance is `available`, prints the **endpoint**, and
   **verifies connectivity** with a raw TCP socket (plus a real login if
   `pymysql` is installed).

Credentials and region are read from `../.env`
(`aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`,
`aws_region_name`).

## Usage

```bash
# Create (master password: 8-41 chars, no  /  "  @  or space)
python main.py create \
  --vpc-id vpc-0123456789abcdef0 \
  --master-password 'SuperSecret123'

# Optionally pin subnets / sizing
python main.py create --vpc-id vpc-... --master-password '...' \
  --subnet-ids subnet-aaa,subnet-bbb \
  --db-name appdb --master-username admin --instance-class db.t3.micro

# Tear everything down
python main.py destroy \
  --db-identifier rds-launcher-db \
  --security-group-id sg-0123456789abcdef0 \
  --subnet-group-name rds-launcher-subnet-group
```

## Connecting from DBeaver / DataGrip

Use the values the tool prints when it finishes:

| Field    | Value                          |
|----------|--------------------------------|
| Host     | the printed **endpoint**       |
| Port     | `3306`                         |
| User     | `admin` (or `--master-username`) |
| Password | your `--master-password`       |
| Database | `appdb` (or `--db-name`)       |

Or from the terminal:

```bash
mysql -h <endpoint> -P 3306 -u admin -p
```

## Notes

- `BackupRetentionPeriod=0` keeps creation fast; remove it for a real DB.
- Opening 3306 to `0.0.0.0/0` is required by this assignment but is **not**
  safe for production — lock it to your IP for real workloads.
- `pip install pymysql` to enable the live login check; otherwise the tool
  confirms reachability with a socket probe and you connect with a GUI tool.

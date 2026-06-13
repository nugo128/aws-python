"""Comfortable RDS (MySQL) launcher CLI (argparse + boto3).

Given a VPC id, this tool:
  * discovers subnets across at least two Availability Zones and builds a DB
    subnet group (RDS requires >= 2 AZs),
  * creates a security group that opens the MySQL port (3306) to ANY IP
    (0.0.0.0/0) so you can connect from DBeaver / DataGrip / mysql CLI,
  * creates a publicly accessible MySQL instance with 60 GB of storage,
  * waits until the instance is 'available', prints the endpoint, and verifies
    connectivity with a raw TCP socket (and a real login if pymysql is present),
  * wraps AWS calls in try/except for friendly error messages.

The 'destroy' subcommand tears everything back down again.
"""

import argparse
import socket
import sys
import time
from os import getenv

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# RDS MySQL master passwords: 8-41 printable chars, none of / " @ or spaces.
PASSWORD_FORBIDDEN = set('/"@ ')


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def init_client(service, region=None):
    return boto3.client(
        service,
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=region or getenv("aws_region_name"),
    )


def aws_fail(action, error):
    """Pretty-print an AWS ClientError and exit."""
    info = error.response.get("Error", {})
    code = info.get("Code", "Unknown")
    message = info.get("Message", str(error))
    sys.exit(f"\n[AWS error] while {action}\n  code:    {code}\n  message: {message}")


def validate_password(pw):
    if not (8 <= len(pw) <= 41):
        sys.exit("Master password must be 8-41 characters long.")
    bad = PASSWORD_FORBIDDEN.intersection(pw)
    if bad:
        sys.exit(f"Master password must not contain any of: / \" @ or space "
                 f"(found: {' '.join(sorted(bad))}).")


# --------------------------------------------------------------------------- #
# Networking: subnets, subnet group, security group
# --------------------------------------------------------------------------- #
def discover_subnets(ec2, vpc_id, explicit_ids=None):
    """Return subnet ids spanning >= 2 AZs (RDS requirement)."""
    try:
        if explicit_ids:
            subnets = ec2.describe_subnets(SubnetIds=explicit_ids)["Subnets"]
        else:
            subnets = ec2.describe_subnets(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            )["Subnets"]
    except ClientError as e:
        aws_fail(f"describing subnets in VPC '{vpc_id}'", e)

    for s in subnets:
        if s["VpcId"] != vpc_id:
            sys.exit(f"Subnet {s['SubnetId']} is not in VPC {vpc_id}.")

    # One subnet per AZ is enough for the subnet group; keep AZ coverage wide.
    by_az = {}
    for s in subnets:
        by_az.setdefault(s["AvailabilityZone"], s["SubnetId"])
    if len(by_az) < 2:
        sys.exit(
            f"Need subnets in at least 2 Availability Zones, found "
            f"{len(by_az)} in VPC {vpc_id}. Create more subnets first."
        )
    chosen = list(by_az.values())
    print(f"  Subnets:   {', '.join(chosen)} "
          f"(AZs: {', '.join(by_az.keys())})")
    return chosen


def create_subnet_group(rds, name, subnet_ids):
    try:
        rds.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="rds-launcher subnet group",
            SubnetIds=subnet_ids,
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "DBSubnetGroupAlreadyExists":
            print(f"  Subnet grp: {name} (already exists, reusing)")
            return name
        aws_fail(f"creating DB subnet group '{name}'", e)
    print(f"  Subnet grp: {name}")
    return name


def create_security_group(ec2, sg_name, vpc_id, port):
    try:
        sg_id = ec2.create_security_group(
            GroupName=sg_name,
            Description="rds-launcher MySQL access",
            VpcId=vpc_id,
        )["GroupId"]
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort": port,
                "ToPort": port,
                # Open to ANY IP, as required by the task.
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "MySQL anywhere"}],
            }],
        )
    except ClientError as e:
        aws_fail(f"creating security group '{sg_name}'", e)
    print(f"  Sec group: {sg_id} (MySQL {port} from 0.0.0.0/0)")
    return sg_id


# --------------------------------------------------------------------------- #
# RDS create / wait
# --------------------------------------------------------------------------- #
def create_db(rds, args, sg_id, subnet_group):
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=args.db_identifier,
            DBName=args.db_name,
            AllocatedStorage=args.allocated_storage,
            DBInstanceClass=args.instance_class,
            Engine=args.engine,
            MasterUsername=args.master_username,
            MasterUserPassword=args.master_password,
            VpcSecurityGroupIds=[sg_id],
            DBSubnetGroupName=subnet_group,
            PubliclyAccessible=True,  # required so external tools can reach it
            Port=args.port,
            BackupRetentionPeriod=0,  # speeds up create; no automated backups
            **({"EngineVersion": args.engine_version} if args.engine_version else {}),
        )
    except ClientError as e:
        aws_fail(f"creating RDS instance '{args.db_identifier}'", e)
    print(f"  RDS:       {args.db_identifier} creating "
          f"({args.engine}, {args.allocated_storage} GB, {args.instance_class})...")


def wait_available(rds, db_identifier):
    print("  Waiting for instance to reach 'available' (a few minutes)...")
    try:
        rds.get_waiter("db_instance_available").wait(
            DBInstanceIdentifier=db_identifier,
            WaiterConfig={"Delay": 15, "MaxAttempts": 80},  # up to ~20 min
        )
        desc = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
    except ClientError as e:
        aws_fail("waiting for the RDS instance", e)
    inst = desc["DBInstances"][0]
    endpoint = inst["Endpoint"]["Address"]
    port = inst["Endpoint"]["Port"]
    print(f"  Available. Endpoint: {endpoint}:{port}")
    return endpoint, port


# --------------------------------------------------------------------------- #
# Connectivity verification
# --------------------------------------------------------------------------- #
def wait_for_port(host, port, timeout=180):
    """Poll the TCP port with a raw socket until it accepts a connection."""
    print(f"  Probing {host}:{port} via socket (up to {timeout}s)...")
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"  Port {port} is OPEN (after {attempt} attempt(s)).")
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(5)
    print(f"  Port {port} did not open within {timeout}s.")
    return False


def try_mysql_login(host, port, user, password):
    """Optional real login if pymysql is installed; otherwise skip cleanly."""
    try:
        import pymysql
    except ImportError:
        print("  (pymysql not installed; skipping live login test. "
              "Use DBeaver/DataGrip with the printed endpoint.)")
        return None
    try:
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, connect_timeout=10)
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
        conn.close()
        print(f"  MySQL login OK. Server version: {version}")
        return True
    except Exception as e:  # pymysql.err.* and socket errors
        print(f"  MySQL login failed: {e}")
        return False


# --------------------------------------------------------------------------- #
# Destroy
# --------------------------------------------------------------------------- #
def destroy(rds, ec2, db_identifier, sg_id, subnet_group):
    if db_identifier:
        try:
            rds.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
            print(f"Deleting RDS instance {db_identifier}...")
            rds.get_waiter("db_instance_deleted").wait(
                DBInstanceIdentifier=db_identifier,
                WaiterConfig={"Delay": 15, "MaxAttempts": 80},
            )
            print("  deleted.")
        except ClientError as e:
            aws_fail(f"deleting RDS instance '{db_identifier}'", e)
    if subnet_group:
        try:
            rds.delete_db_subnet_group(DBSubnetGroupName=subnet_group)
            print(f"Deleted DB subnet group {subnet_group}.")
        except ClientError as e:
            aws_fail(f"deleting DB subnet group '{subnet_group}'", e)
    if sg_id:
        try:
            ec2.delete_security_group(GroupId=sg_id)
            print(f"Deleted security group {sg_id}.")
        except ClientError as e:
            aws_fail(f"deleting security group '{sg_id}'", e)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Create a publicly reachable MySQL RDS instance (60 GB) with "
                    "a security group open on 3306, then verify connectivity.",
    )
    parser.add_argument("--region", default=None,
                        help="AWS region (defaults to aws_region_name in .env).")
    sub = parser.add_subparsers(dest="action", required=True)

    c = sub.add_parser("create", help="Provision RDS + SG and connectivity-test.")
    c.add_argument("--vpc-id", required=True, help="Target VPC id.")
    c.add_argument("--subnet-ids", default=None,
                   help="Comma-separated subnet ids (>=2 AZs). "
                        "Omit to auto-discover from the VPC.")
    c.add_argument("--master-password", required=True,
                   help="Master password (8-41 chars; no / \" @ or space).")
    c.add_argument("--db-identifier", default="rds-launcher-db",
                   help="RDS instance identifier.")
    c.add_argument("--db-name", default="appdb",
                   help="Initial database name to create.")
    c.add_argument("--master-username", default="admin",
                   help="Master username (default: admin).")
    c.add_argument("--engine", default="mysql", help="DB engine (default: mysql).")
    c.add_argument("--engine-version", default=None,
                   help="Engine version (default: AWS default for the engine).")
    c.add_argument("--instance-class", default="db.t3.micro",
                   help="Instance class (default: db.t3.micro).")
    c.add_argument("--allocated-storage", type=int, default=60,
                   help="Storage in GB (default: 60).")
    c.add_argument("--port", type=int, default=3306,
                   help="Database port (default: 3306).")
    c.add_argument("--sg-name", default="rds-launcher-sg",
                   help="Security group name to create.")
    c.add_argument("--subnet-group-name", default="rds-launcher-subnet-group",
                   help="DB subnet group name to create.")
    c.add_argument("--port-timeout", type=int, default=300,
                   help="Seconds to wait for the DB port (default: 300).")

    d = sub.add_parser("destroy", help="Tear down resources created by 'create'.")
    d.add_argument("--db-identifier", help="RDS instance to delete.")
    d.add_argument("--security-group-id", help="Security group to delete.")
    d.add_argument("--subnet-group-name", help="DB subnet group to delete.")

    args = parser.parse_args()
    ec2 = init_client("ec2", args.region)
    rds = init_client("rds", args.region)

    if args.action == "create":
        validate_password(args.master_password)
        explicit = ([s.strip() for s in args.subnet_ids.split(",") if s.strip()]
                    if args.subnet_ids else None)

        print("Networking...")
        subnet_ids = discover_subnets(ec2, args.vpc_id, explicit)
        subnet_group = create_subnet_group(rds, args.subnet_group_name, subnet_ids)
        sg_id = create_security_group(ec2, args.sg_name, args.vpc_id, args.port)

        print("\nProvisioning RDS...")
        create_db(rds, args, sg_id, subnet_group)
        endpoint, port = wait_available(rds, args.db_identifier)

        print("\nVerifying connectivity...")
        reachable = wait_for_port(endpoint, port, args.port_timeout)
        try_mysql_login(endpoint, port, args.master_username, args.master_password)

        print("\nDone:")
        print(f"  endpoint   : {endpoint}")
        print(f"  port       : {port}")
        print(f"  username   : {args.master_username}")
        print(f"  database   : {args.db_name}")
        print(f"  port {port} : {'reachable' if reachable else 'not yet reachable'}")
        print(f"  mysql cli  : mysql -h {endpoint} -P {port} "
              f"-u {args.master_username} -p")
        print("\nCleanup when done:")
        print(f"  python main.py destroy --db-identifier {args.db_identifier} "
              f"--security-group-id {sg_id} "
              f"--subnet-group-name {subnet_group}")

    elif args.action == "destroy":
        destroy(rds, ec2, args.db_identifier, args.security_group_id,
                args.subnet_group_name)


if __name__ == "__main__":
    main()

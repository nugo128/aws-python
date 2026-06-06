"""Comfortable EC2 launcher CLI (argparse + boto3).

Given a VPC id and a subnet id, this tool:
  * validates both ids against AWS,
  * dynamically resolves the latest official Amazon Linux 2023 AMI,
  * opens SSH (22) in a new security group only to your IP (auto-detected, or
    --custom-ssh-ip),
  * creates a key pair and writes the .pem with 0400 permissions,
  * launches the instance, waits until it is running, and confirms port 22 is
    reachable using a raw socket,
  * wraps AWS calls in try/except for friendly error messages.

There is no "Amazon Linux 3"; AL2023 is the newest Amazon Linux line (it
succeeds Amazon Linux 2), so that is what `--ami-name-pattern` defaults to.
"""

import argparse
import ipaddress
import os
import socket
import sys
import time
import urllib.request
from os import getenv

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

DEFAULT_AMI_PATTERN = "al2023-ami-2023.*-x86_64"
# Public services used to discover the caller's external IP, tried in order.
IP_SERVICES = (
    "https://checkip.amazonaws.com",
    "https://api.ipify.org",
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def init_ec2(region=None):
    return boto3.client(
        "ec2",
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


def detect_external_ip():
    """Return the caller's public IPv4 by querying a public echo service."""
    for url in IP_SERVICES:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                ip = resp.read().decode().strip()
            ipaddress.IPv4Address(ip)  # validate
            return ip
        except (urllib.error.URLError, ValueError, OSError):
            continue
    sys.exit("Could not auto-detect your external IP. Pass --custom-ssh-ip instead.")


def normalize_cidr(value):
    """Accept '1.2.3.4' or '1.2.3.4/32' or a CIDR; return a valid CIDR string."""
    try:
        if "/" not in value:
            value = f"{value}/32"
        return str(ipaddress.IPv4Network(value, strict=False))
    except (ValueError, ipaddress.AddressValueError) as e:
        sys.exit(f"Invalid SSH IP/CIDR '{value}': {e}")


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate_vpc(ec2, vpc_id):
    try:
        ec2.describe_vpcs(VpcIds=[vpc_id])
    except ClientError as e:
        aws_fail(f"validating VPC '{vpc_id}'", e)
    print(f"  VPC ok:    {vpc_id}")


def validate_subnet(ec2, subnet_id, vpc_id):
    try:
        subnets = ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"]
    except ClientError as e:
        aws_fail(f"validating subnet '{subnet_id}'", e)
    subnet = subnets[0]
    if subnet["VpcId"] != vpc_id:
        sys.exit(
            f"Subnet {subnet_id} belongs to VPC {subnet['VpcId']}, "
            f"not {vpc_id}."
        )
    print(f"  Subnet ok: {subnet_id} ({subnet['AvailabilityZone']})")
    return subnet


# --------------------------------------------------------------------------- #
# AMI
# --------------------------------------------------------------------------- #
def latest_amazon_linux_ami(ec2, pattern):
    try:
        images = ec2.describe_images(
            Owners=["amazon"],
            Filters=[
                {"Name": "name", "Values": [pattern]},
                {"Name": "state", "Values": ["available"]},
                {"Name": "architecture", "Values": ["x86_64"]},
                {"Name": "root-device-type", "Values": ["ebs"]},
            ],
        )["Images"]
    except ClientError as e:
        aws_fail("searching for the latest Amazon Linux AMI", e)
    if not images:
        sys.exit(f"No AMI matched pattern '{pattern}'.")
    newest = max(images, key=lambda i: i["CreationDate"])
    print(f"  Latest AMI: {newest['ImageId']} ({newest['Name']})")
    return newest["ImageId"]


# --------------------------------------------------------------------------- #
# Key pair + security group
# --------------------------------------------------------------------------- #
def create_key_pair(ec2, key_name, pem_path):
    try:
        key = ec2.create_key_pair(KeyName=key_name)
    except ClientError as e:
        aws_fail(f"creating key pair '{key_name}'", e)
    with open(pem_path, "w") as fh:
        fh.write(key["KeyMaterial"])
    os.chmod(pem_path, 0o400)  # required: read-only for owner
    print(f"  Key pair:  {key_name} -> {pem_path} (chmod 0400)")


def create_security_group(ec2, sg_name, vpc_id, ssh_cidr):
    try:
        sg_id = ec2.create_security_group(
            GroupName=sg_name,
            Description="ec2-launcher SSH access",
            VpcId=vpc_id,
        )["GroupId"]
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": ssh_cidr, "Description": "SSH"}],
            }],
        )
    except ClientError as e:
        aws_fail(f"creating security group '{sg_name}'", e)
    print(f"  Sec group: {sg_id} (SSH 22 from {ssh_cidr})")
    return sg_id


# --------------------------------------------------------------------------- #
# Launch + connectivity
# --------------------------------------------------------------------------- #
def launch_instance(ec2, ami_id, instance_type, key_name, sg_id, subnet_id, name):
    try:
        reservation = ec2.run_instances(
            ImageId=ami_id,
            InstanceType=instance_type,
            KeyName=key_name,
            MaxCount=1,
            MinCount=1,
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "SubnetId": subnet_id,
                "Groups": [sg_id],
                "AssociatePublicIpAddress": True,  # ensure a reachable public IP
            }],
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": name}],
            }],
        )
    except ClientError as e:
        aws_fail("launching the instance", e)
    instance_id = reservation["Instances"][0]["InstanceId"]
    print(f"  Instance:  {instance_id} launching...")
    return instance_id


def wait_running(ec2, instance_id):
    print("  Waiting for instance to reach 'running'...")
    try:
        ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        desc = ec2.describe_instances(InstanceIds=[instance_id])
    except ClientError as e:
        aws_fail("waiting for the instance", e)
    inst = desc["Reservations"][0]["Instances"][0]
    public_ip = inst.get("PublicIpAddress")
    print(f"  Running. Public IP: {public_ip}")
    return public_ip


def wait_for_port(host, port=22, timeout=180):
    """Poll the TCP port with a raw socket until it accepts a connection."""
    if not host:
        sys.exit("Instance has no public IP; cannot test SSH connectivity.")
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
    print(f"  Port {port} did not open within {timeout}s "
          f"(instance is up; SSH may still be booting).")
    return False


# --------------------------------------------------------------------------- #
# Destroy (convenience cleanup)
# --------------------------------------------------------------------------- #
def destroy(ec2, instance_id, sg_id, key_name, pem_file):
    if instance_id:
        try:
            ec2.terminate_instances(InstanceIds=[instance_id])
            print(f"Terminating {instance_id}...")
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            print("  terminated.")
        except ClientError as e:
            aws_fail(f"terminating '{instance_id}'", e)
    if sg_id:
        try:
            ec2.delete_security_group(GroupId=sg_id)
            print(f"Deleted security group {sg_id}.")
        except ClientError as e:
            aws_fail(f"deleting security group '{sg_id}'", e)
    if key_name:
        try:
            ec2.delete_key_pair(KeyName=key_name)
            print(f"Deleted key pair {key_name}.")
        except ClientError as e:
            aws_fail(f"deleting key pair '{key_name}'", e)
    if pem_file and os.path.exists(pem_file):
        os.remove(pem_file)
        print(f"Removed {pem_file}.")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Launch an Amazon Linux 2023 EC2 instance with SSH locked "
                    "to your IP, and verify port 22 is reachable.",
    )
    parser.add_argument("--region", default=None,
                        help="AWS region (defaults to aws_region_name in .env).")
    sub = parser.add_subparsers(dest="action", required=True)

    c = sub.add_parser("create", help="Validate, launch and connectivity-test.")
    c.add_argument("--vpc-id", required=True, help="Target VPC id (validated).")
    c.add_argument("--subnet-id", required=True, help="Target subnet id (validated).")
    c.add_argument("--instance-type", default="t3.micro", help="Default: t3.micro.")
    c.add_argument("--key-name", default="ec2-launcher-key",
                   help="Key pair name; .pem saved as <key-name>.pem.")
    c.add_argument("--sg-name", default="ec2-launcher-sg",
                   help="Security group name to create.")
    c.add_argument("--name", default="ec2-launcher-instance",
                   help="Name tag for the instance.")
    c.add_argument("--custom-ssh-ip", default=None,
                   help="Override the auto-detected SSH IP (IP or CIDR).")
    c.add_argument("--ami-name-pattern", default=DEFAULT_AMI_PATTERN,
                   help=f"AMI name filter (default: {DEFAULT_AMI_PATTERN}).")
    c.add_argument("--port-timeout", type=int, default=180,
                   help="Seconds to wait for port 22 (default: 180).")

    d = sub.add_parser("destroy", help="Tear down resources created by 'create'.")
    d.add_argument("--instance-id", help="Instance to terminate.")
    d.add_argument("--security-group-id", help="Security group to delete.")
    d.add_argument("--key-name", help="Key pair to delete.")
    d.add_argument("--pem-file", help="Local .pem file to remove.")

    args = parser.parse_args()
    ec2 = init_ec2(args.region)

    if args.action == "create":
        ssh_cidr = (normalize_cidr(args.custom_ssh_ip) if args.custom_ssh_ip
                    else f"{detect_external_ip()}/32")

        print("Validating inputs...")
        validate_vpc(ec2, args.vpc_id)
        validate_subnet(ec2, args.subnet_id, args.vpc_id)
        print(f"  SSH from:  {ssh_cidr}")

        print("\nProvisioning...")
        ami_id = latest_amazon_linux_ami(ec2, args.ami_name_pattern)
        pem_path = f"{args.key_name}.pem"
        create_key_pair(ec2, args.key_name, pem_path)
        sg_id = create_security_group(ec2, args.sg_name, args.vpc_id, ssh_cidr)

        instance_id = launch_instance(
            ec2, ami_id, args.instance_type, args.key_name, sg_id,
            args.subnet_id, args.name,
        )
        public_ip = wait_running(ec2, instance_id)
        reachable = wait_for_port(public_ip, 22, args.port_timeout)

        print("\nDone:")
        print(f"  instance-id : {instance_id}")
        print(f"  public-ip   : {public_ip}")
        print(f"  ssh         : ssh -i {pem_path} ec2-user@{public_ip}")
        print(f"  port 22     : {'reachable' if reachable else 'not yet reachable'}")
        print("\nCleanup when done:")
        print(f"  python main.py destroy --instance-id {instance_id} "
              f"--security-group-id {sg_id} --key-name {args.key_name} "
              f"--pem-file {pem_path}")

    elif args.action == "destroy":
        destroy(ec2, args.instance_id, args.security_group_id,
                args.key_name, args.pem_file)


if __name__ == "__main__":
    main()

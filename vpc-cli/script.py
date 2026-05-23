import argparse
import ipaddress
import sys
from os import getenv

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def init_ec2_client(region=None):
    return boto3.client(
        "ec2",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=region or getenv("aws_region_name"),
    )


def validate_cidr(cidr, label):
    try:
        return str(ipaddress.IPv4Network(cidr, strict=True))
    except (ValueError, ipaddress.AddressValueError) as e:
        sys.exit(f"Invalid {label} CIDR '{cidr}': {e}")


def name_tag(name):
    return [{"ResourceType": "vpc", "Tags": [{"Key": "Name", "Value": name}]}]


def tag_spec(resource_type, name):
    return [{"ResourceType": resource_type, "Tags": [{"Key": "Name", "Value": name}]}]


def find_vpc(ec2, vpc_name):
    response = ec2.describe_vpcs(
        Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]
    )
    vpcs = response.get("Vpcs", [])
    if not vpcs:
        return None
    return vpcs[0]


def create_infrastructure(ec2, vpc_name, vpc_cidr, public_cidr, private_cidr):
    if find_vpc(ec2, vpc_name):
        sys.exit(f"VPC named '{vpc_name}' already exists. Choose another name or run destroy first.")

    print(f"Creating VPC '{vpc_name}' ({vpc_cidr})...")
    vpc = ec2.create_vpc(
        CidrBlock=vpc_cidr,
        TagSpecifications=tag_spec("vpc", vpc_name),
    )["Vpc"]
    vpc_id = vpc["VpcId"]
    ec2.get_waiter("vpc_available").wait(VpcIds=[vpc_id])
    print(f"  VPC created: {vpc_id}")

    print("Creating Internet Gateway...")
    igw = ec2.create_internet_gateway(
        TagSpecifications=tag_spec("internet-gateway", f"{vpc_name}-igw"),
    )["InternetGateway"]
    igw_id = igw["InternetGatewayId"]
    ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    print(f"  IGW created and attached: {igw_id}")

    print(f"Creating public subnet ({public_cidr})...")
    public_subnet = ec2.create_subnet(
        VpcId=vpc_id,
        CidrBlock=public_cidr,
        TagSpecifications=tag_spec("subnet", f"{vpc_name}-public"),
    )["Subnet"]
    public_subnet_id = public_subnet["SubnetId"]
    print(f"  Public subnet: {public_subnet_id}")

    print(f"Creating private subnet ({private_cidr})...")
    private_subnet = ec2.create_subnet(
        VpcId=vpc_id,
        CidrBlock=private_cidr,
        TagSpecifications=tag_spec("subnet", f"{vpc_name}-private"),
    )["Subnet"]
    private_subnet_id = private_subnet["SubnetId"]
    print(f"  Private subnet: {private_subnet_id}")

    print("Creating public route table...")
    public_rt = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=tag_spec("route-table", f"{vpc_name}-public-rt"),
    )["RouteTable"]
    public_rt_id = public_rt["RouteTableId"]
    ec2.create_route(
        RouteTableId=public_rt_id,
        DestinationCidrBlock="0.0.0.0/0",
        GatewayId=igw_id,
    )
    ec2.associate_route_table(RouteTableId=public_rt_id, SubnetId=public_subnet_id)
    print(f"  Public route table: {public_rt_id} (0.0.0.0/0 -> {igw_id})")

    print("Creating private route table...")
    private_rt = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=tag_spec("route-table", f"{vpc_name}-private-rt"),
    )["RouteTable"]
    private_rt_id = private_rt["RouteTableId"]
    ec2.associate_route_table(RouteTableId=private_rt_id, SubnetId=private_subnet_id)
    print(f"  Private route table: {private_rt_id}")

    print("\nInfrastructure created successfully:")
    print(f"  VPC          {vpc_id}")
    print(f"  IGW          {igw_id}")
    print(f"  Public sub   {public_subnet_id}  ({public_cidr})")
    print(f"  Private sub  {private_subnet_id}  ({private_cidr})")
    print(f"  Public RT    {public_rt_id}")
    print(f"  Private RT   {private_rt_id}")


def destroy_infrastructure(ec2, vpc_name):
    vpc = find_vpc(ec2, vpc_name)
    if not vpc:
        sys.exit(f"No VPC found with Name tag '{vpc_name}'.")
    vpc_id = vpc["VpcId"]
    print(f"Found VPC '{vpc_name}': {vpc_id}")

    igws = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )["InternetGateways"]
    for igw in igws:
        igw_id = igw["InternetGatewayId"]
        print(f"Detaching IGW {igw_id}...")
        ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        print(f"Deleting IGW {igw_id}...")
        ec2.delete_internet_gateway(InternetGatewayId=igw_id)

    subnets = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["Subnets"]
    for subnet in subnets:
        subnet_id = subnet["SubnetId"]
        print(f"Deleting subnet {subnet_id}...")
        ec2.delete_subnet(SubnetId=subnet_id)

    route_tables = ec2.describe_route_tables(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["RouteTables"]
    for rt in route_tables:
        is_main = any(a.get("Main") for a in rt.get("Associations", []))
        if is_main:
            continue
        rt_id = rt["RouteTableId"]
        for assoc in rt.get("Associations", []):
            if not assoc.get("Main"):
                ec2.disassociate_route_table(AssociationId=assoc["RouteTableAssociationId"])
        print(f"Deleting route table {rt_id}...")
        ec2.delete_route_table(RouteTableId=rt_id)

    print(f"Deleting VPC {vpc_id}...")
    ec2.delete_vpc(VpcId=vpc_id)
    print("\nDestroyed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Create or destroy a small AWS VPC (public + private subnet, IGW).",
    )
    parser.add_argument("--region", default=None, help="AWS region (defaults to aws_region_name in .env).")
    subparsers = parser.add_subparsers(dest="action", required=True)

    create_parser = subparsers.add_parser("create", help="Create the VPC stack.")
    create_parser.add_argument("--vpc-name", required=True)
    create_parser.add_argument("--vpc-cidr", default="10.0.0.0/16")
    create_parser.add_argument("--public-cidr", default="10.0.1.0/24")
    create_parser.add_argument("--private-cidr", default="10.0.2.0/24")

    destroy_parser = subparsers.add_parser("destroy", help="Destroy the VPC stack by name tag.")
    destroy_parser.add_argument("--vpc-name", required=True)

    args = parser.parse_args()

    if args.action == "create":
        vpc_cidr = validate_cidr(args.vpc_cidr, "VPC")
        public_cidr = validate_cidr(args.public_cidr, "public subnet")
        private_cidr = validate_cidr(args.private_cidr, "private subnet")

        vpc_net = ipaddress.IPv4Network(vpc_cidr)
        for label, sub in (("public", public_cidr), ("private", private_cidr)):
            if not ipaddress.IPv4Network(sub).subnet_of(vpc_net):
                sys.exit(f"{label} subnet {sub} is not inside VPC CIDR {vpc_cidr}.")
        if ipaddress.IPv4Network(public_cidr).overlaps(ipaddress.IPv4Network(private_cidr)):
            sys.exit("Public and private subnet CIDRs overlap.")

        ec2 = init_ec2_client(args.region)
        try:
            create_infrastructure(ec2, args.vpc_name, vpc_cidr, public_cidr, private_cidr)
        except ClientError as e:
            sys.exit(f"AWS error: {e}")

    elif args.action == "destroy":
        ec2 = init_ec2_client(args.region)
        try:
            destroy_infrastructure(ec2, args.vpc_name)
        except ClientError as e:
            sys.exit(f"AWS error: {e}")


if __name__ == "__main__":
    main()

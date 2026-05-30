import argparse
import ipaddress
import math
import sys
from os import getenv

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# AWS default soft limit: 200 subnets per VPC. Going above this requires opening
# a support case and asking AWS to raise the limit exclusively for your account.
MAX_SUBNETS_PER_VPC = 200


def init_ec2_client(region=None):
    return boto3.client(
        "ec2",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=region or getenv("aws_region_name"),
    )


def positive_subnet_count(value):
    """argparse type: N subnets per tier, capped so the VPC total stays <= 200."""
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not an integer.")
    if n < 1:
        raise argparse.ArgumentTypeError("number of subnets must be at least 1.")
    # We create N public + N private subnets, so the total is 2 * N.
    if 2 * n > MAX_SUBNETS_PER_VPC:
        raise argparse.ArgumentTypeError(
            f"requesting {n} public + {n} private = {2 * n} subnets exceeds the "
            f"AWS limit of {MAX_SUBNETS_PER_VPC} subnets per VPC "
            f"(max -n is {MAX_SUBNETS_PER_VPC // 2}). To go higher you must open "
            f"an AWS support case and request an exclusive limit increase."
        )
    return n


def validate_cidr(cidr, label):
    try:
        return str(ipaddress.IPv4Network(cidr, strict=True))
    except (ValueError, ipaddress.AddressValueError) as e:
        sys.exit(f"Invalid {label} CIDR '{cidr}': {e}")


def tag_spec(resource_type, name):
    return [{"ResourceType": resource_type, "Tags": [{"Key": "Name", "Value": name}]}]


def find_vpc(ec2, vpc_name):
    response = ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [vpc_name]}])
    vpcs = response.get("Vpcs", [])
    return vpcs[0] if vpcs else None


def carve_subnets(vpc_cidr, total_subnets, subnet_prefix=None):
    """Split the VPC CIDR into `total_subnets` equally sized, non-overlapping blocks.

    If subnet_prefix is None, pick the smallest prefix that yields enough blocks.
    Returns a list of CIDR strings.
    """
    vpc_net = ipaddress.IPv4Network(vpc_cidr)

    if subnet_prefix is None:
        # Need at least `total_subnets` blocks -> add ceil(log2(total)) host bits.
        extra_bits = max(1, math.ceil(math.log2(total_subnets)))
        subnet_prefix = vpc_net.prefixlen + extra_bits

    if subnet_prefix <= vpc_net.prefixlen:
        sys.exit(
            f"subnet prefix /{subnet_prefix} must be longer (more specific) than "
            f"the VPC prefix /{vpc_net.prefixlen}."
        )
    if subnet_prefix > 32:
        sys.exit(f"subnet prefix /{subnet_prefix} is invalid (max /32).")

    available = 2 ** (subnet_prefix - vpc_net.prefixlen)
    if available < total_subnets:
        sys.exit(
            f"VPC {vpc_cidr} split into /{subnet_prefix} blocks yields only "
            f"{available} subnets, but {total_subnets} were requested. "
            f"Use a larger VPC CIDR or a longer --subnet-prefix."
        )

    blocks = list(vpc_net.subnets(new_prefix=subnet_prefix))
    return [str(b) for b in blocks[:total_subnets]]


def list_azs(ec2):
    azs = ec2.describe_availability_zones(
        Filters=[{"Name": "state", "Values": ["available"]}]
    )["AvailabilityZones"]
    return [az["ZoneName"] for az in azs]


def create_infrastructure(ec2, vpc_name, vpc_cidr, num_subnets, subnet_prefix):
    if find_vpc(ec2, vpc_name):
        sys.exit(
            f"VPC named '{vpc_name}' already exists. "
            f"Choose another name or run destroy first."
        )

    total = num_subnets * 2  # N public + N private
    cidrs = carve_subnets(vpc_cidr, total, subnet_prefix)
    public_cidrs = cidrs[:num_subnets]
    private_cidrs = cidrs[num_subnets:]

    azs = list_azs(ec2)
    if not azs:
        sys.exit("No available Availability Zones found in this region.")

    print(f"Creating VPC '{vpc_name}' ({vpc_cidr})...")
    vpc = ec2.create_vpc(
        CidrBlock=vpc_cidr,
        TagSpecifications=tag_spec("vpc", vpc_name),
    )["Vpc"]
    vpc_id = vpc["VpcId"]
    ec2.get_waiter("vpc_available").wait(VpcIds=[vpc_id])
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    print(f"  VPC created: {vpc_id}")

    print("Creating Internet Gateway...")
    igw_id = ec2.create_internet_gateway(
        TagSpecifications=tag_spec("internet-gateway", f"{vpc_name}-igw"),
    )["InternetGateway"]["InternetGatewayId"]
    ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    print(f"  IGW created and attached: {igw_id}")

    # Public route table: a default route to the Internet Gateway makes any subnet
    # associated with it "public" (its instances are reachable from the internet).
    print("Creating public route table (0.0.0.0/0 -> IGW)...")
    public_rt_id = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=tag_spec("route-table", f"{vpc_name}-public-rt"),
    )["RouteTable"]["RouteTableId"]
    ec2.create_route(
        RouteTableId=public_rt_id,
        DestinationCidrBlock="0.0.0.0/0",
        GatewayId=igw_id,
    )
    print(f"  Public route table: {public_rt_id}")

    # Private route table: NO route to the IGW, so associated subnets stay isolated
    # from inbound internet traffic. This is the core public/private difference.
    print("Creating private route table (no internet route)...")
    private_rt_id = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=tag_spec("route-table", f"{vpc_name}-private-rt"),
    )["RouteTable"]["RouteTableId"]
    print(f"  Private route table: {private_rt_id}")

    public_ids = []
    print(f"\nCreating {num_subnets} public subnet(s)...")
    for i, cidr in enumerate(public_cidrs):
        az = azs[i % len(azs)]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=cidr,
            AvailabilityZone=az,
            TagSpecifications=tag_spec("subnet", f"{vpc_name}-public-{i + 1}"),
        )["Subnet"]["SubnetId"]
        # Auto-assign a public IPv4 to instances launched here (public behavior).
        ec2.modify_subnet_attribute(
            SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True}
        )
        ec2.associate_route_table(RouteTableId=public_rt_id, SubnetId=subnet_id)
        public_ids.append(subnet_id)
        print(f"  public  #{i + 1}: {subnet_id}  {cidr}  {az}")

    private_ids = []
    print(f"\nCreating {num_subnets} private subnet(s)...")
    for i, cidr in enumerate(private_cidrs):
        az = azs[i % len(azs)]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=cidr,
            AvailabilityZone=az,
            TagSpecifications=tag_spec("subnet", f"{vpc_name}-private-{i + 1}"),
        )["Subnet"]["SubnetId"]
        ec2.associate_route_table(RouteTableId=private_rt_id, SubnetId=subnet_id)
        private_ids.append(subnet_id)
        print(f"  private #{i + 1}: {subnet_id}  {cidr}  {az}")

    print("\nInfrastructure created successfully:")
    print(f"  VPC             {vpc_id}  ({vpc_cidr})")
    print(f"  IGW             {igw_id}")
    print(f"  Public subnets  {len(public_ids)}  -> RT {public_rt_id}")
    print(f"  Private subnets {len(private_ids)}  -> RT {private_rt_id}")
    print(f"  Total subnets   {len(public_ids) + len(private_ids)}")


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
        print(f"Detaching and deleting IGW {igw_id}...")
        ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
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
        if any(a.get("Main") for a in rt.get("Associations", [])):
            continue  # the main route table is deleted together with the VPC
        rt_id = rt["RouteTableId"]
        for assoc in rt.get("Associations", []):
            if not assoc.get("Main"):
                ec2.disassociate_route_table(
                    AssociationId=assoc["RouteTableAssociationId"]
                )
        print(f"Deleting route table {rt_id}...")
        ec2.delete_route_table(RouteTableId=rt_id)

    print(f"Deleting VPC {vpc_id}...")
    ec2.delete_vpc(VpcId=vpc_id)
    print("\nDestroyed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Create or destroy an AWS VPC with N public and N private subnets "
            "(IGW-routed public subnets, isolated private subnets)."
        ),
    )
    parser.add_argument(
        "--region", default=None,
        help="AWS region (defaults to aws_region_name in .env).",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    create_parser = subparsers.add_parser("create", help="Create the VPC stack.")
    create_parser.add_argument("--vpc-name", required=True, help="Name tag for the VPC.")
    create_parser.add_argument(
        "--vpc-cidr", default="10.0.0.0/16",
        help="VPC CIDR block (default: 10.0.0.0/16).",
    )
    create_parser.add_argument(
        "-n", "--num-subnets", type=positive_subnet_count, default=1,
        help=(
            "Number of subnets PER TIER. Creates N public + N private subnets. "
            f"Total is capped at {MAX_SUBNETS_PER_VPC} per VPC "
            f"(so max N is {MAX_SUBNETS_PER_VPC // 2})."
        ),
    )
    create_parser.add_argument(
        "--subnet-prefix", type=int, default=None,
        help=(
            "Optional fixed prefix length for each subnet (e.g. 24). "
            "If omitted, it is calculated automatically from the VPC CIDR."
        ),
    )

    destroy_parser = subparsers.add_parser(
        "destroy", help="Destroy the VPC stack by name tag."
    )
    destroy_parser.add_argument("--vpc-name", required=True)

    args = parser.parse_args()
    ec2 = init_ec2_client(args.region)

    if args.action == "create":
        vpc_cidr = validate_cidr(args.vpc_cidr, "VPC")
        try:
            create_infrastructure(
                ec2, args.vpc_name, vpc_cidr, args.num_subnets, args.subnet_prefix
            )
        except ClientError as e:
            sys.exit(f"AWS error: {e}")

    elif args.action == "destroy":
        try:
            destroy_infrastructure(ec2, args.vpc_name)
        except ClientError as e:
            sys.exit(f"AWS error: {e}")


if __name__ == "__main__":
    main()

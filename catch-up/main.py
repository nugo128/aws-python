import logging
from botocore.exceptions import ClientError
from auth import init_client
from bucket.crud import list_buckets, create_bucket, delete_bucket, bucket_exists
from bucket.policy import read_bucket_policy, assign_policy, disable_public_access_block, enable_static_website
from object.crud import (
    download_file_and_upload_to_s3, get_objects, upload_file,
    upload_file_multipart, delete_object, get_versioning_status,
    list_object_versions, restore_previous_version, upload_file_by_type,
    delete_old_versions, upload_directory,
)
from object.policy import set_lifecycle_policy
from bucket.encryption import set_bucket_encryption, read_bucket_encryption
from inspire import get_random_quote, get_quote_by_author, save_quote_to_s3
import argparse

parser = argparse.ArgumentParser(
    description="CLI program that helps with S3 buckets.",
    usage='''
    How to download and upload directly:
    short:
        python main.py -bn new-bucket-btu-7 -ol https://cdn.activestate.com/wp-content/uploads/2021/12/python-coding-mistakes.jpg -du
    long:
       python main.py  --bucket_name new-bucket-btu-7 --object_link https://cdn.activestate.com/wp-content/uploads/2021/12/python-coding-mistakes.jpg --download_upload

    How to list buckets:
    short:
        python main.py -lb
    long:
        python main.py --list_buckets

    How to create bucket:
    short:
        -bn new-bucket-btu-1 -cb -region us-west-2
    long:
        --bucket_name new-bucket-btu-1 --create_bucket --region us-west-2

    How to assign missing policy:
    short:
        -bn new-bucket-btu-1 -amp
    long:
        --bn new-bucket-btu-1 --assign_missing_policy
    ''',
    prog="main.py",
    epilog="DEMO APP FOR BTU_AWS",
)

parser.add_argument(
    "-lb",
    "--list_buckets",
    help="List already created buckets.",
    # https://docs.python.org/dev/library/argparse.html#action
    action="store_true",
)

parser.add_argument(
    "-cb",
    "--create_bucket",
    help="Flag to create bucket.",
    choices=["False", "True"],
    type=str,
    nargs="?",
    # https://jdhao.github.io/2018/10/11/python_argparse_set_boolean_params
    const="True",
    default="False",
)

parser.add_argument(
    "-bn", "--bucket_name", type=str, help="Pass bucket name.", default=None
)

parser.add_argument(
    "-bc",
    "--bucket_check",
    help="Check if bucket already exists.",
    choices=["False", "True"],
    type=str,
    nargs="?",
    const="True",
    default="True",
)

parser.add_argument(
    "-region", "--region", type=str, help="Region variable.", default=None
)

parser.add_argument(
    "-db",
    "--delete_bucket",
    help="flag to delete bucket",
    choices=["False", "True"],
    type=str,
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-be",
    "--bucket_exists",
    help="flag to check if bucket exists",
    choices=["False", "True"],
    type=str,
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-rp",
    "--read_policy",
    help="flag to read bucket policy.",
    choices=["False", "True"],
    type=str,
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-arp",
    "--assign_read_policy",
    help="flag to assign read bucket policy.",
    choices=["False", "True"],
    type=str,
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-amp",
    "--assign_missing_policy",
    help="flag to assign read bucket policy.",
    choices=["False", "True"],
    type=str,
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-du",
    "--download_upload",
    choices=["False", "True"],
    help="download and upload to bucket",
    type=str,
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-ol",
    "--object_link",
    type=str,
    help="link to download and upload to bucket",
    default=None,
)

parser.add_argument(
    "-lo",
    "--list_objects",
    type=str,
    help="list bucket object",
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-ben",
    "--bucket_encryption",
    type=str,
    help="bucket object encryption",
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-rben",
    "--read_bucket_encryption",
    type=str,
    help="list bucket object",
    nargs="?",
    const="True",
    default="False",
)

parser.add_argument(
    "-uf",
    "--upload_file",
    type=str,
    help="Upload a small file to the bucket. Pass file path.",
    default=None,
)

parser.add_argument(
    "-ufm",
    "--upload_file_multipart",
    type=str,
    help="Upload a large file using multipart upload. Pass file path.",
    default=None,
)

parser.add_argument(
    "-uft",
    "--upload_file_typed",
    type=str,
    help="Upload a file to a bucket folder chosen by python-magic detected MIME type. Pass file path.",
    default=None,
)

parser.add_argument(
    "-mv",
    "--mime_validation",
    help="Enable mimetype validation during upload.",
    action="store_true",
)

parser.add_argument(
    "-lp",
    "--lifecycle_policy",
    help="Set lifecycle policy to delete objects after 120 days.",
    action="store_true",
)

parser.add_argument(
    "-del",
    "--delete_object",
    help="Delete a specific object from the bucket.",
    action="store_true",
)

parser.add_argument(
    "-key",
    "--object_key",
    type=str,
    help="Object key (file name) in the bucket.",
    default=None,
)

parser.add_argument(
    "-vs",
    "--versioning_status",
    help="Check if versioning is enabled on the bucket.",
    action="store_true",
)

parser.add_argument(
    "-lv",
    "--list_versions",
    help="List all versions of a file. Requires -key.",
    action="store_true",
)

parser.add_argument(
    "-rv",
    "--restore_version",
    help="Restore the previous version of a file as the new latest. Requires -key.",
    action="store_true",
)

parser.add_argument(
    "-host",
    "--host_static",
    type=str,
    help="Host a static site on the bucket. Pass the local directory to upload (e.g. static-react/dist).",
    default=None,
)

parser.add_argument(
    "-dov",
    "--delete_old_versions",
    nargs="+",
    help="Delete all versions of the given object key(s) that are older than 6 months. Example: -dov file1.txt file2.pdf",
    default=None,
)

parser.add_argument(
    "--inspire",
    type=str,
    help="Get an inspiring quote. Pass author name to filter, or 'random' for any quote.",
    nargs="?",
    const="random",
    default=None,
)

parser.add_argument(
    "-save",
    "--save_quote",
    help="Save the inspire quote as .json to the bucket. Requires -bn.",
    action="store_true",
)

parser.add_argument(
    "-dpab",
    "--disable_public_access_block",
    type=str,
    help="Disable public access block",
    nargs="?",
    const="True",
    default="False",
)


def main():
    args = parser.parse_args()

    if args.region:
        s3_client = init_client(region_name=args.region)
    else:
        s3_client = init_client()


    if args.bucket_name:
        if args.disable_public_access_block == "True":
            disable_public_access_block(s3_client, args.bucket_name)

        if args.create_bucket == "True":
            if not args.region:
                parser.error("Please provide region for bucket --region REGION_NAME")
            if (args.bucket_check == "True") and bucket_exists(
                s3_client, args.bucket_name
            ):
                parser.error("Bucket already exists")
            if create_bucket(s3_client, args.bucket_name, args.region):
                print("Bucket successfully created")

        if (args.delete_bucket == "True") and delete_bucket(
            s3_client, args.bucket_name
        ):
            print("Bucket successfully deleted")

        if args.bucket_exists == "True":
            print(f"Bucket exists: {bucket_exists(s3_client, args.bucket_name)}")

        if args.read_policy == "True":
            print(read_bucket_policy(s3_client, args.bucket_name))

        if args.assign_read_policy == "True":
            assign_policy(s3_client, "public_read_policy", args.bucket_name)

        if args.assign_missing_policy == "True":
            assign_policy(s3_client, "multiple_policy", args.bucket_name)

        if args.object_link:
            if args.download_upload == "True":
                print(
                    download_file_and_upload_to_s3(
                        s3_client, args.bucket_name, args.object_link
                    )
                )
        if args.bucket_encryption == "True":
            if set_bucket_encryption(s3_client, args.bucket_name):
                print("Encryption set")
        if args.read_bucket_encryption == "True":
            print(read_bucket_encryption(s3_client, args.bucket_name))

        if args.list_objects == "True":
            get_objects(s3_client, args.bucket_name)

        if args.upload_file:
            upload_file(s3_client, args.upload_file, args.bucket_name, validate_mime=args.mime_validation)

        if args.upload_file_multipart:
            upload_file_multipart(s3_client, args.upload_file_multipart, args.bucket_name, validate_mime=args.mime_validation)

        if args.upload_file_typed:
            upload_file_by_type(s3_client, args.upload_file_typed, args.bucket_name)

        if args.lifecycle_policy:
            set_lifecycle_policy(s3_client, args.bucket_name)

        if args.delete_object:
            if not args.object_key:
                parser.error("Please provide object key with -key OBJECT_KEY")
            delete_object(s3_client, args.bucket_name, args.object_key)

        if args.versioning_status:
            get_versioning_status(s3_client, args.bucket_name)

        if args.list_versions:
            if not args.object_key:
                parser.error("Please provide object key with -key OBJECT_KEY")
            list_object_versions(s3_client, args.bucket_name, args.object_key)

        if args.restore_version:
            if not args.object_key:
                parser.error("Please provide object key with -key OBJECT_KEY")
            restore_previous_version(s3_client, args.bucket_name, args.object_key)

        if args.delete_old_versions:
            delete_old_versions(s3_client, args.bucket_name, args.delete_old_versions)

        if args.host_static:
            print(f"Preparing bucket '{args.bucket_name}' for static hosting...")
            try:
                disable_public_access_block(s3_client, args.bucket_name)
            except ClientError as e:
                print(f"  (public access block already disabled: {e.response['Error']['Code']})")
            assign_policy(s3_client, "public_read_policy", args.bucket_name)
            upload_directory(s3_client, args.host_static, args.bucket_name)
            enable_static_website(s3_client, args.bucket_name)

    if args.inspire is not None:
        if args.inspire == "random":
            quote = get_random_quote()
        else:
            quote = get_quote_by_author(args.inspire)
        if quote:
            print(f'\n  "{quote["content"]}"\n  — {quote["author"]}\n')
            if args.save_quote:
                if not args.bucket_name:
                    parser.error("Please provide bucket name with -bn BUCKET_NAME to save quote")
                save_quote_to_s3(s3_client, args.bucket_name, quote)

    if args.list_buckets:
        buckets = list_buckets(s3_client)
        if buckets:
            for bucket in buckets["Buckets"]:
                print(f'  {bucket["Name"]}')


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        logging.error(e)

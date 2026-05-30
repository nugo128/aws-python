"""myauto.ge -> S3 CLI tool.

A single argparse-based CLI that can:
  1. scrape  : download every car image from the first N page(s) of myauto.ge
  2. upload  : recursively upload a local directory of images to an S3 bucket
  3. all     : do both in one shot (scrape, then upload)

Images landing in the configured S3 bucket trigger the Rekognition Lambda
(see ./rekognition/handler.py) which stores label analysis in DynamoDB.

Built on the lecture scraper (lecture_8/optional/myauto_scrapper.py) and the
lecture_6 rekognition project, but reorganised into one CLI with no third-party
HTTP dependency (standard-library urllib only).
"""

import argparse
import concurrent.futures as futures
import os
import sys
import urllib.error
import urllib.request
import zipfile
from json import loads
from os import getenv

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# myauto.ge public product API (paginated) and the static image host pattern,
# taken from the lecture scraper.
API_URL = (
    "https://api2.myauto.ge/ka/products?TypeID=0&ForRent=&Mans="
    "&CurrencyID=3&MileageType=1&Page={page}"
)
IMAGE_URL = "https://static.my.ge/myauto/photos/{photo}/large/{car_id}_{n}.jpg"

# myauto's API/CDN reject requests without a browser-like User-Agent + Referer.
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ka-GE,ka;q=0.9,en;q=0.8",
    "Referer": "https://www.myauto.ge/",
}

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png")


# --------------------------------------------------------------------------- #
# myauto.ge scraping
# --------------------------------------------------------------------------- #
def fetch_page(page):
    """Return the list of car items for a given myauto.ge page number."""
    req = urllib.request.Request(API_URL.format(page=page), headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = loads(resp.read())
    return payload.get("data", {}).get("items", [])


def image_targets(item, all_photos):
    """Yield (url, filename) pairs for one car item.

    By default only the main photo (index 1) of each car is taken so that we
    get "every car's image"; --all-photos grabs the full gallery instead.
    """
    car_id = item["car_id"]
    photo = item["photo"]
    count = item.get("pic_number") or 1
    indexes = range(1, count + 1) if all_photos else [1]
    for n in indexes:
        url = IMAGE_URL.format(photo=photo, car_id=car_id, n=n)
        yield url, f"{car_id}_{n}.jpg"


def download_image(url, dest_path):
    """Download a single image to dest_path. Returns True on success."""
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        with open(dest_path, "wb") as fh:
            fh.write(data)
        return True
    except (urllib.error.URLError, OSError) as e:
        print(f"  ! failed {url}: {e}")
        return False


def scrape(pages, output_dir, all_photos, workers):
    """Download images from the first `pages` page(s) of myauto.ge."""
    os.makedirs(output_dir, exist_ok=True)

    jobs = []
    for page in range(1, pages + 1):
        print(f"Fetching listing page {page}...")
        try:
            items = fetch_page(page)
        except (urllib.error.URLError, ValueError) as e:
            sys.exit(f"Failed to fetch page {page}: {e}")
        print(f"  {len(items)} cars on page {page}")
        for item in items:
            for url, filename in image_targets(item, all_photos):
                jobs.append((url, os.path.join(output_dir, filename)))

    print(f"\nDownloading {len(jobs)} image(s) with {workers} workers...")
    ok = 0
    with futures.ThreadPoolExecutor(max_workers=workers) as pool:
        results = pool.map(lambda job: download_image(*job), jobs)
        ok = sum(1 for r in results if r)
    print(f"Downloaded {ok}/{len(jobs)} images into '{output_dir}'.")
    return ok


def make_zip(output_dir):
    archive = f"{output_dir.rstrip('/')}.zip"
    print(f"Archiving '{output_dir}' -> '{archive}'...")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(output_dir):
            for name in files:
                full = os.path.join(root, name)
                zf.write(full, os.path.relpath(full, output_dir))
    print(f"  created {archive}")
    return archive


# --------------------------------------------------------------------------- #
# S3 upload
# --------------------------------------------------------------------------- #
def init_s3_client(region=None):
    return boto3.client(
        "s3",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=region or getenv("aws_region_name"),
    )


def content_type_for(path):
    ext = os.path.splitext(path)[1].lower()
    return {"png": "image/png"}.get(ext.lstrip("."), "image/jpeg")


def upload_directory(s3, source_dir, bucket, prefix):
    """Recursively upload every image under source_dir to the S3 bucket."""
    if not os.path.isdir(source_dir):
        sys.exit(f"Source directory '{source_dir}' does not exist.")

    uploaded = 0
    for root, _, files in os.walk(source_dir):  # os.walk => recursive
        for name in files:
            if not name.lower().endswith(IMAGE_EXTENSIONS):
                continue
            full = os.path.join(root, name)
            rel = os.path.relpath(full, source_dir)
            key = f"{prefix.rstrip('/')}/{rel}" if prefix else rel
            key = key.replace(os.sep, "/")
            try:
                s3.upload_file(
                    full, bucket, key,
                    ExtraArgs={"ContentType": content_type_for(name)},
                )
                uploaded += 1
                print(f"  uploaded s3://{bucket}/{key}")
            except ClientError as e:
                print(f"  ! failed {full}: {e}")
    print(f"Uploaded {uploaded} image(s) to bucket '{bucket}'.")
    return uploaded


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def add_scrape_args(p):
    p.add_argument("--pages", type=int, default=1,
                   help="Number of listing pages to scrape (default: 1).")
    p.add_argument("--output-dir", default="downloaded_images",
                   help="Directory to save images (default: downloaded_images).")
    p.add_argument("--all-photos", action="store_true",
                   help="Download every photo of each car (default: main photo only).")
    p.add_argument("--workers", type=int, default=10,
                   help="Concurrent download workers (default: 10).")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape myauto.ge car images and upload them to S3.",
    )
    parser.add_argument("--region", default=None,
                        help="AWS region (defaults to aws_region_name in .env).")
    sub = parser.add_subparsers(dest="action", required=True)

    scrape_p = sub.add_parser("scrape", help="Download car images from myauto.ge.")
    add_scrape_args(scrape_p)
    scrape_p.add_argument("--zip", action="store_true",
                          help="Also create a .zip archive of the images.")

    upload_p = sub.add_parser("upload", help="Recursively upload a directory to S3.")
    upload_p.add_argument("--source-dir", default="downloaded_images",
                          help="Local directory to upload (default: downloaded_images).")
    upload_p.add_argument("--bucket", required=True, help="Target S3 bucket name.")
    upload_p.add_argument("--prefix", default="",
                          help="S3 key prefix/folder (default: bucket root).")

    all_p = sub.add_parser("all", help="Scrape myauto.ge, then upload to S3.")
    add_scrape_args(all_p)
    all_p.add_argument("--bucket", required=True, help="Target S3 bucket name.")
    all_p.add_argument("--prefix", default="", help="S3 key prefix/folder.")

    args = parser.parse_args()

    if args.action == "scrape":
        scrape(args.pages, args.output_dir, args.all_photos, args.workers)
        if args.zip:
            make_zip(args.output_dir)

    elif args.action == "upload":
        s3 = init_s3_client(args.region)
        upload_directory(s3, args.source_dir, args.bucket, args.prefix)

    elif args.action == "all":
        scrape(args.pages, args.output_dir, args.all_photos, args.workers)
        s3 = init_s3_client(args.region)
        upload_directory(s3, args.output_dir, args.bucket, args.prefix)


if __name__ == "__main__":
    main()

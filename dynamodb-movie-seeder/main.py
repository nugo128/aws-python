"""DynamoDB movie seeder — a 1:1 port of the MySQL `seed_mysql_rds_with_movie_data`
seeder, plus a bonus mood-based recommender.

Original MySQL functions -> DynamoDB equivalents
------------------------------------------------
  check_connection            -> ping DynamoDB (list_tables)
  CREATE DATABASE/TABLE        -> create_table  (a single DynamoDB table)
  INSERT ... executemany       -> seed_movies   (batch_writer put_item)
  view_movies_by_genre         -> view_movies_by_genre   (scan + contains filter)
  suggest_movies_by_cast       -> suggest_movies_by_cast (scan + contains filter)

Bonus: recommend_by_mood — maps a mood to genres and returns 2 random movies.

MySQL stored `genres`/`cast` as comma-joined strings and matched with
FIND_IN_SET(); DynamoDB stores them as native lists and matches with the
`contains()` filter, which is the direct equivalent.

Credentials/region come from ../.env (same convention as the other tools).
"""

import argparse
import json
import random
import sys
import urllib.request
from os import getenv

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "1990s_movies"
MOVIES_URL = (
    "https://raw.githubusercontent.com/prust/wikipedia-movie-data/"
    "master/movies-1990s.json"
)

# Mood -> genres mapping for the bonus recommender.
MOODS = {
    "happy": ["Comedy", "Romance", "Adventure"],
    "sad": ["Drama", "Romance"],
    "excited": ["Action", "Adventure", "Thriller"],
    "scary": ["Horror", "Thriller"],
    "thoughtful": ["Drama", "Science Fiction"],
    "intrigued": ["Mystery", "Thriller"],
    "nostalgic": ["Drama", "Romance"],
    "inspired": ["Biography", "Drama"],
    "mysterious": ["Thriller", "Mystery"],
    "action-packed": ["Action", "Adventure", "Science Fiction"],
}


# --------------------------------------------------------------------------- #
# Clients / helpers
# --------------------------------------------------------------------------- #
def init_resource(region=None):
    return boto3.resource(
        "dynamodb",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=region or getenv("aws_region_name"),
    )


def init_client(region=None):
    return boto3.client(
        "dynamodb",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=region or getenv("aws_region_name"),
    )


def aws_fail(action, error):
    info = error.response.get("Error", {})
    code = info.get("Code", "Unknown")
    message = info.get("Message", str(error))
    sys.exit(f"\n[AWS error] while {action}\n  code:    {code}\n  message: {message}")


def scan_all(table, filter_cond, limit=None):
    """Scan the whole table applying a filter, following pagination.

    DynamoDB's analogue of `SELECT * ... WHERE ...`. Limit stops early once
    enough *matching* items are collected (unlike DynamoDB's Limit, which caps
    items examined before the filter)."""
    items = []
    kwargs = {"FilterExpression": filter_cond}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp["Items"])
        if limit and len(items) >= limit:
            return items[:limit]
        last = resp.get("LastEvaluatedKey")
        if not last:
            return items
        kwargs["ExclusiveStartKey"] = last


# --------------------------------------------------------------------------- #
# check_connection  (MySQL: conn.ping())
# --------------------------------------------------------------------------- #
def check_connection(client):
    try:
        client.list_tables(Limit=1)
        return True
    except ClientError as e:
        print("Error connecting to DynamoDB:", e)
        return False


# --------------------------------------------------------------------------- #
# create_table  (MySQL: CREATE DATABASE / USE / CREATE TABLE)
# --------------------------------------------------------------------------- #
def create_table(client):
    """Create the movies table if it does not exist; wait until ACTIVE.

    Key schema mirrors a movie's natural identity: title (HASH) + year (RANGE).
    On-demand billing keeps it free at rest."""
    try:
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "title", "KeyType": "HASH"},
                {"AttributeName": "year", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "title", "AttributeType": "S"},
                {"AttributeName": "year", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"  Creating table {TABLE_NAME}...")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceInUseException":
            print(f"  Table {TABLE_NAME} already exists, reusing.")
        else:
            aws_fail(f"creating table '{TABLE_NAME}'", e)
    try:
        client.get_waiter("table_exists").wait(TableName=TABLE_NAME)
    except ClientError as e:
        aws_fail("waiting for the table to become active", e)
    print(f"  Table {TABLE_NAME} is ready.")


# --------------------------------------------------------------------------- #
# seed_movies  (MySQL: fetch JSON + executemany INSERT + commit)
# --------------------------------------------------------------------------- #
def fetch_movies():
    print(f"  Downloading movie data from {MOVIES_URL}")
    with urllib.request.urlopen(MOVIES_URL, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def to_item(movie):
    """Build a DynamoDB item, mirroring the MySQL column set. None/empty values
    are dropped (DynamoDB rejects null attributes)."""
    title = movie.get("title")
    year = movie.get("year")
    if not title or year is None:  # both are key attributes; skip if missing
        return None
    item = {
        "title": title,
        "year": int(year),
        # native lists replace MySQL's comma-joined FIND_IN_SET strings
        "genres": movie.get("genres", []),
        "cast": movie.get("cast", []),
    }
    for field in ("href", "extract", "thumbnail"):
        value = movie.get(field)
        if value:
            item[field] = value
    for field in ("thumbnail_width", "thumbnail_height"):
        value = movie.get(field)
        if value is not None:
            item[field] = int(value)
    return item


def seed_movies(table):
    data = fetch_movies()
    written = skipped = 0
    # batch_writer auto-batches (25/req) and retries; overwrite_by_pkeys de-dupes
    # items sharing a key within the run.
    with table.batch_writer(overwrite_by_pkeys=["title", "year"]) as batch:
        for movie in data:
            item = to_item(movie)
            if item is None:
                skipped += 1
                continue
            batch.put_item(Item=item)
            written += 1
    print(f"seed - successful ({written} movies written, {skipped} skipped)")


# --------------------------------------------------------------------------- #
# view_movies_by_genre  (MySQL: FIND_IN_SET(genre, genres) LIMIT 5)
# --------------------------------------------------------------------------- #
def view_movies_by_genre(table, genre, limit=5):
    return scan_all(table, Attr("genres").contains(genre), limit=limit)


# --------------------------------------------------------------------------- #
# suggest_movies_by_cast  (MySQL: FIND_IN_SET(cast_member, cast))
# --------------------------------------------------------------------------- #
def suggest_movies_by_cast(table, cast_member, limit=5):
    return scan_all(table, Attr("cast").contains(cast_member), limit=limit)


# --------------------------------------------------------------------------- #
# BONUS: recommend_by_mood — 2 random movies that match the mood's genres
# --------------------------------------------------------------------------- #
def recommend_by_mood(table, mood, count=2):
    genres = MOODS.get(mood)
    if not genres:
        sys.exit(f"Unknown mood '{mood}'. Choose one of: {', '.join(MOODS)}")
    # Match a movie if ANY of the mood's genres is in its genres list.
    condition = Attr("genres").contains(genres[0])
    for genre in genres[1:]:
        condition = condition | Attr("genres").contains(genre)
    matches = scan_all(table, condition)
    if not matches:
        print(f"No movies found for mood '{mood}'.")
        return []
    return random.sample(matches, min(count, len(matches)))


# --------------------------------------------------------------------------- #
# Pretty printing
# --------------------------------------------------------------------------- #
def print_movies(movies):
    if not movies:
        print("  (no movies found)")
        return
    for m in movies:
        genres = ", ".join(m.get("genres", [])) or "-"
        print(f"  - {m.get('title')} ({m.get('year')}) [{genres}]")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Seed a DynamoDB table with 1990s movies and query it "
                    "(DynamoDB port of the MySQL movie seeder).",
    )
    parser.add_argument("--region", default=None,
                        help="AWS region (defaults to aws_region_name in .env).")
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("seed", help="Create the table and load all 1990s movies.")

    g = sub.add_parser("by-genre", help="List movies for a genre (limit 5).")
    g.add_argument("genre", help="e.g. Comedy, Action, Drama")

    c = sub.add_parser("by-cast", help="Suggest movies featuring a cast member.")
    c.add_argument("cast_member", help="e.g. 'Tom Hanks'")

    m = sub.add_parser("mood", help="Recommend 2 random movies for a mood.")
    m.add_argument("mood", choices=sorted(MOODS), help="Your current mood.")

    sub.add_parser("destroy", help="Delete the DynamoDB table.")

    args = parser.parse_args()
    client = init_client(args.region)
    resource = init_resource(args.region)
    table = resource.Table(TABLE_NAME)

    if not check_connection(client):
        sys.exit(1)

    if args.action == "seed":
        create_table(client)
        seed_movies(table)

    elif args.action == "by-genre":
        print(f"Movies in genre '{args.genre}':")
        print_movies(view_movies_by_genre(table, args.genre))

    elif args.action == "by-cast":
        print(f"Movies featuring '{args.cast_member}':")
        print_movies(suggest_movies_by_cast(table, args.cast_member))

    elif args.action == "mood":
        print(f"Because you feel '{args.mood}', try these "
              f"({', '.join(MOODS[args.mood])}):")
        print_movies(recommend_by_mood(table, args.mood))

    elif args.action == "destroy":
        try:
            client.delete_table(TableName=TABLE_NAME)
            client.get_waiter("table_not_exists").wait(TableName=TABLE_NAME)
            print(f"Deleted table {TABLE_NAME}.")
        except ClientError as e:
            aws_fail(f"deleting table '{TABLE_NAME}'", e)


if __name__ == "__main__":
    main()

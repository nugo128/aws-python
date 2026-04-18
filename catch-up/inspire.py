import json
import random
import urllib.request
import urllib.parse
from datetime import datetime, timezone


API_BASE = "https://api.quotable.kurokeita.dev/api"


def _api_get(path):
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def get_random_quote():
    data = _api_get("/quotes/random")
    quote = data["quote"]
    return {
        "content": quote["content"],
        "author": quote["author"]["name"],
    }


def get_quote_by_author(author_name):
    encoded = urllib.parse.quote(author_name)
    data = _api_get(f"/quotes?author={encoded}")
    quotes = data.get("data", [])
    if not quotes:
        print(f"No quotes found for author '{author_name}'")
        return None
    chosen = random.choice(quotes)
    return {
        "content": chosen["content"],
        "author": chosen["author"]["name"],
    }


def save_quote_to_s3(aws_s3_client, bucket_name, quote):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = quote["author"].lower().replace(" ", "_")
    key = f"quotes/{slug}_{timestamp}.json"
    body = json.dumps(quote, ensure_ascii=False, indent=2)
    aws_s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Saved quote to s3://{bucket_name}/{key}")
    return key

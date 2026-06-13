# dynamodb-movie-seeder

A DynamoDB port of the MySQL `seed_mysql_rds_with_movie_data.py` seeder, built
with `argparse` + `boto3`. It loads the public
[wikipedia 1990s movies dataset](https://github.com/prust/wikipedia-movie-data)
into a DynamoDB table and lets you query it.

## MySQL → DynamoDB mapping

| MySQL seeder | DynamoDB port |
|---|---|
| `conn.ping()` (`check_connection`) | `check_connection` → `list_tables` ping |
| `CREATE DATABASE` / `USE` / `CREATE TABLE` | `create_table` → one table, on-demand billing |
| `INSERT ... executemany` + `commit` | `seed_movies` → `batch_writer.put_item` |
| `view_movies_by_genre` (`FIND_IN_SET(genre, genres)`) | `view_movies_by_genre` → `scan` + `Attr("genres").contains()` |
| `suggest_movies_by_cast` (`FIND_IN_SET(cast, cast)`) | `suggest_movies_by_cast` → `scan` + `Attr("cast").contains()` |

MySQL stored `genres`/`cast` as comma-joined strings matched with
`FIND_IN_SET`. DynamoDB stores them as native **lists** matched with the
`contains()` filter — the direct equivalent.

**Bonus:** `recommend_by_mood` maps a mood to genres and returns **2 random**
movies whose genres match.

## Table

- Name: `1990s_movies`
- Key: `title` (HASH) + `year` (RANGE)
- Billing: `PAY_PER_REQUEST` (free at rest, no capacity to manage)

Credentials/region come from `../.env`.

## Usage

```bash
python main.py seed                  # create table + load ~2,800 movies
python main.py by-genre Comedy       # up to 5 movies in a genre
python main.py by-cast "Tom Hanks"   # movies featuring a cast member
python main.py mood happy            # 2 random movies for a mood
python main.py destroy               # delete the table
```

Available moods: `happy, sad, excited, scary, thoughtful, intrigued,
nostalgic, inspired, mysterious, action-packed`.

## Notes

- `scan` reads the whole table to apply `contains()` filters. Fine for this
  dataset (~2,800 items); for production you'd add a GSI or denormalize.
- On-demand DynamoDB costs effectively nothing at rest, but run
  `python main.py destroy` to remove the table when you're done.

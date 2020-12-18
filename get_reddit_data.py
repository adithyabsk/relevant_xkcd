"""Get relevant xkcd comments from Reddit."""

from pathlib import Path
import os

from tqdm import tqdm
from google.cloud import bigquery
from google.cloud import bigquery_storage

CREDS_PATH = Path("~/.config/gcloud_creds.json").expanduser()
DATA_PATH = Path(__file__).parent / 'data' / 'reddit'
QUERY_STRING = """
SELECT self.body body, self.author author, self.score score, 
       CONCAT('http://reddit.com/r/', self.subreddit,'/comments/', REGEXP_REPLACE(self.link_id, r't[0-9]_', ''), '/c/', self.id) as permalink,
       REGEXP_EXTRACT(self.body, r'https?:\/\/(?:(?:w{{3}}|m)\.)?xkcd\.com\/\d+\/?') xkcd,
       parent.body parent_body,
       parent.author parent_author,
       parent.score parent_score,
       CONCAT('http://reddit.com/r/', parent.subreddit,'/comments/', REGEXP_REPLACE(parent.link_id, r't[0-9]_', ''), '/c/', parent.id) as parent_permalink,
FROM `fh-bigquery.reddit_comments.{table_name}` self, `fh-bigquery.reddit_comments.{table_name}` parent
WHERE REGEXP_CONTAINS(self.body, r'((http)s?:\/\/(((www)|m)\.)?xkcd\.com\/\d+\/?)')
AND self.score >= 1
AND parent.score >= 1
AND REGEXP_REPLACE(self.parent_id, r't[0-9]_', '') = parent.id;
"""
# this query is a self join that allows use to get both the a comment that has an xkcd url and its parent along with
# attributes of both. We filter out any posts that have been downvoted. Note that we do the a REGEXP_REPLACE instead of
# a straight compare using self.name because the field is null after


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDS_PATH)


def get_reddit_comments_table():
    """Get data from the BigQuery Reddit comments dataset."""
    # https://www.reddit.com/r/bigquery/comments/3cej2b/17_billion_reddit_comments_loaded_on_bigquery/
    client = bigquery.Client()
    storage_client = bigquery_storage.BigQueryReadClient()
    # 2005 & 2006 have no samples
    table_names = [
        *list(map(str, range(2007, 2015))),
        *[f"{year}_{month:02d}" for year in range(2015, 2020) for month in range(1, 13)]
    ]
    for table_name in tqdm(table_names):
        tqdm.write(f"{table_name}")
        df = client.query(QUERY_STRING.format(table_name=table_name)).result().to_dataframe(bqstorage_client=storage_client)
        df.to_csv(DATA_PATH / f"{table_name}.csv", index=False)


if __name__ == "__main__":
    get_reddit_comments_table()

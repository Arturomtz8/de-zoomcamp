
from pathlib import Path
from typing import List

import nltk
import pandas as pd
import praw
import pyarrow
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_gcp.cloud_storage import GcsBucket

# todo manage concatening two dfs: read from gcs, and concat


@task(log_prints=True)
def get_posts_id() -> List[str]:
    path = Path(f"../data/ghost_stories/posts_ghosts_stories.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    
    try:
        df = pd.read_parquet(path)
        post_ids_list = df["post_id"].tolist()
    except pyarrow.lib.ArrowInvalid:
        return list()

    return post_ids_list


@task(tags="extract reddit posts")
def extract_posts(subreddit_name: str, posts_id_list: List[str]) -> pd.DataFrame:
    all_posts_list = list()
    REDDIT_CLIENT_ID = Secret.load("reddit-client-id")
    REDDIT_CLIENT_SECRET = Secret.load("reddit-client-secret")
    REDDIT_USER_AGENT = Secret.load("reddit-user-agent")
    REDDIT_USERNAME = Secret.load("reddit-username")
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID.get(),
        client_secret=REDDIT_CLIENT_SECRET.get(),
        user_agent=REDDIT_USER_AGENT.get(),
        username=REDDIT_USERNAME.get(),
    )


    subreddit = reddit.subreddit(subreddit_name)

    
    for submission in subreddit.hot(limit=10):
        if str(submission.id) not in posts_id_list:
            print("found new posts")
            titles = submission.title
            text = submission.selftext
            scores = submission.score
            url = submission.url
            created_at = submission.created_utc
            num_comments = submission.num_comments
            over_18 = submission.over_18

            post_preview = {
                "post_id": str(submission.id),
                "post_title": str(titles),
                "post_text": str(text),
                "post_score": str(scores),
                "post_url": str(url),
                "created_at": float(created_at),
                "num_comments": str(num_comments),
                "over_18": bool(over_18)
            }
            all_posts_list.append(post_preview)


    df_raw = pd.DataFrame(all_posts_list)
    if df_raw.empty:
        raise Exception('no new data available')

    return df_raw


@task(log_prints=True)
def clean_df(df: pd.DataFrame)  -> pd.DataFrame:
    df['created_at'] = pd.to_datetime(df['created_at'],unit='s')
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame) -> Path:
    path = Path(f"../data/ghost_stories/posts_ghosts_stories.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("bucket-zoomcamp")
    # remove ../ from gcs path
    gcs_path = str(path).split("/")[1:]
    gcs_path = '/'.join(gcs_path)
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)


@flow()
def scrape_reddit():
    posts_id_list = get_posts_id()
    df_raw = extract_posts("Ghoststories", posts_id_list)
    df = clean_df(df_raw)
    path = write_local(df)
    write_gcs(path)




if __name__ == "__main__":
    scrape_reddit()

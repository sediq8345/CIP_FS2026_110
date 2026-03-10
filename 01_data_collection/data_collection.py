"""
CIP FS2026 - Group 110
Data Collection Script
Source: Arctic Shift API (https://arctic-shift.photon-reddit.com)
Subreddits: r/ChatGPT, r/ClaudeAI, r/Gemini
Time window: last 6 months
"""

import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
import os

# ── Configuration ─────────────────────────────────────────────────────────────

SUBREDDITS = ["ChatGPT", "ClaudeAI", "Gemini"]

# Same time window applied to all subreddits for consistency
SIX_MONTHS_AGO = datetime.now(tz=timezone.utc) - timedelta(days=180)
NOW            = datetime.now(tz=timezone.utc)

# Convert to Unix timestamps (required by Arctic Shift API)
START_TS = int(SIX_MONTHS_AGO.timestamp())
END_TS   = int(NOW.timestamp())

# Arctic Shift API base URL
BASE_URL = "https://arctic-shift.photon-reddit.com/api"

# Output file
OUTPUT_FILE = "reddit_data_raw.csv"

# How often to save progress (every N new entries collected)
AUTOSAVE_INTERVAL = 5000

# Delay between API requests in seconds (to respect rate limits)
REQUEST_DELAY = 2


# ── Helper: Save DataFrame to CSV ─────────────────────────────────────────────

def save_progress(df, filepath):
    """
    Save the current DataFrame to CSV.
    Uses utf-8-sig encoding for Excel compatibility.
    """
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"  [Saved] {len(df)} entries written to '{filepath}'")


# ── Helper: Fetch one page from Arctic Shift API ──────────────────────────────

def fetch_page(endpoint, subreddit, after_ts, end_ts):
    """
    Sends a GET request to the Arctic Shift API.
    Returns a list of items (dicts) or an empty list on failure.

    Parameters:
        endpoint  (str): 'comments' or 'posts'
        subreddit (str): subreddit name without 'r/'
        after_ts  (int): Unix timestamp - start of the window
        end_ts    (int): Unix timestamp - end of the window

    Returns:
        list: items returned by the API (may be empty)
    """
    url = (
        f"{BASE_URL}/{endpoint}/search"
        f"?subreddit={subreddit}"
        f"&after={after_ts}"
        f"&before={end_ts}"
        f"&limit=100"
        f"&sort=asc"
    )

    try:
        response = requests.get(url, timeout=30)
    except Exception as error:
        print(f"  [Connection error] {error} - retrying in 30s...")
        time.sleep(30)
        return []

    if response.status_code == 429:
        print("  [Rate limited] Waiting 60 seconds...")
        time.sleep(60)
        return []

    if response.status_code != 200:
        print(f"  [HTTP {response.status_code}] Stopping '{endpoint}' collection.")
        return None  # None signals a hard stop

    return response.json().get("data", [])


# ── Helper: Parse a single comment item into a row dict ───────────────────────

def parse_comment(item, subreddit):
    """
    Extracts relevant fields from a raw API comment dict.
    Returns a dict with standardized column names.
    """
    created = datetime.fromtimestamp(int(item["created_utc"]), tz=timezone.utc)
    return {
        "subreddit" : subreddit,
        "post_id"   : item.get("link_id", "").replace("t3_", ""),
        "post_title": "",   # not available in comment endpoint
        "type"      : "comment",
        "text"      : item.get("body", ""),
        "date"      : created.strftime("%Y-%m-%d"),
    }


# ── Helper: Parse a single post item into a row dict ──────────────────────────

def parse_post(item, subreddit):
    """
    Extracts relevant fields from a raw API post dict.
    Returns a dict with standardized column names.
    """
    created = datetime.fromtimestamp(int(item["created_utc"]), tz=timezone.utc)
    return {
        "subreddit" : subreddit,
        "post_id"   : item.get("id", ""),
        "post_title": item.get("title", ""),
        "type"      : "post",
        "text"      : item.get("selftext", ""),
        "date"      : created.strftime("%Y-%m-%d"),
    }


# ── Core collection function ───────────────────────────────────────────────────

def collect_entries(subreddit, endpoint, parse_fn, existing_df):
    """
    Collects all entries (comments or posts) from one subreddit
    for the configured 6 month time window.

    Uses pagination: each request shifts the time window forward
    to the timestamp of the last received item.

    Autosaves progress every AUTOSAVE_INTERVAL new entries.

    Parameters:
        subreddit   (str):       subreddit name
        endpoint    (str):       'comments' or 'posts'
        parse_fn    (function):  parse_comment or parse_post
        existing_df (DataFrame): already collected data (for autosave merging)

    Returns:
        list of dicts: all collected rows
    """
    rows      = []
    after_ts  = START_TS
    last_save = 0

    print(f"\n  Collecting {endpoint} from r/{subreddit}...")

    while after_ts < END_TS:

        items = fetch_page(endpoint, subreddit, after_ts, END_TS)

        # Hard stop (non-200 response)
        if items is None:
            break

        # No more items in this time window
        if len(items) == 0:
            print(f"  No more {endpoint}.")
            break

        # Parse each item and append to rows list
        for item in items:
            rows.append(parse_fn(item, subreddit))

        # Move the time window forward past the last item
        new_after_ts = int(items[-1]["created_utc"]) + 1

        # Safety check: if timestamp is not advancing, break to avoid infinite loop
        if new_after_ts <= after_ts:
            print(f"  [Warning] Timestamp not advancing (stuck at {datetime.fromtimestamp(after_ts, tz=timezone.utc).strftime('%Y-%m-%d')}) - moving on.")
            break

        after_ts = new_after_ts

        # Print progress
        last_date = datetime.fromtimestamp(after_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        print(f"  {len(rows):>7} {endpoint} collected up to {last_date}")

        # Autosave: merge new rows with existing data and save
        if len(rows) - last_save >= AUTOSAVE_INTERVAL:
            new_df    = pd.DataFrame(rows)
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
            save_progress(merged_df, OUTPUT_FILE)
            last_save = len(rows)

        time.sleep(REQUEST_DELAY)

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():

    print("=" * 60)
    print("CIP FS2026 - Reddit Data Collection via Arctic Shift API")
    print("=" * 60)
    print(f"Time window : {SIX_MONTHS_AGO.strftime('%Y-%m-%d')} to {NOW.strftime('%Y-%m-%d')}")
    print(f"Subreddits  : {', '.join(['r/' + s for s in SUBREDDITS])}")
    print(f"Output file : {OUTPUT_FILE}")
    print(f"Autosave    : every {AUTOSAVE_INTERVAL} new entries")
    print("=" * 60)

    # Load existing data if the output file already exists (resume support)
    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
        print(f"\n[Resume] Found existing file with {len(existing_df)} entries.")
    else:
        existing_df = pd.DataFrame(columns=["subreddit", "post_id", "post_title", "type", "text", "date"])
        print("\n[Start] No existing file found - starting fresh.")

    all_rows = []

    # ── Collect from each subreddit ───────────────────────────────────────────
    for subreddit in SUBREDDITS:

        print(f"\n{'=' * 60}")
        print(f"Subreddit: r/{subreddit}")
        print(f"{'=' * 60}")

        # Collect comments
        comment_rows = collect_entries(
            subreddit   = subreddit,
            endpoint    = "comments",
            parse_fn    = parse_comment,
            existing_df = existing_df
        )

        # Collect posts (selftext only - no link posts)
        post_rows = collect_entries(
            subreddit   = subreddit,
            endpoint    = "posts",
            parse_fn    = parse_post,
            existing_df = existing_df
        )

        all_rows.extend(comment_rows)
        all_rows.extend(post_rows)

        # Per-subreddit summary using DataFrame.info() style approach
        sub_df = pd.DataFrame(comment_rows + post_rows)
        if not sub_df.empty:
            print(f"\n  r/{subreddit} summary:")
            print(f"    Comments  : {len(comment_rows)}")
            print(f"    Posts     : {len(post_rows)}")
            print(f"    Total     : {len(sub_df)}")
            print(f"    Date range: {sub_df['date'].min()} to {sub_df['date'].max()}")

    # ── Final save ────────────────────────────────────────────────────────────
    final_df = pd.concat([existing_df, pd.DataFrame(all_rows)], ignore_index=True)

    # Remove potential duplicates (safety check in case of resume from autosave)
    final_df = final_df.drop_duplicates(subset=["subreddit", "post_id", "type", "text"])

    save_progress(final_df, OUTPUT_FILE)

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("COLLECTION COMPLETE")
    print(f"{'=' * 60}")
    print(f"\nTotal entries saved : {len(final_df)}")

    print("\nBreakdown by subreddit and type:")
    print(final_df.groupby(["subreddit", "type"]).size().to_string())

    print("\nDate range per subreddit:")
    print(final_df.groupby("subreddit")["date"].agg(["min", "max"]).to_string())

    print("\nDataFrame info:")
    final_df.info()

    print("\nMissing values per column:")
    print(final_df.isnull().sum().to_string())

    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    main()



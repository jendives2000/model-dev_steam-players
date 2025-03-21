import os
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent
STATE_FILE = BASE_DIR / "last_start_account.txt"


def steamid_from_accountid(account_id):
    """Convert a 32-bit account ID to a 64-bit SteamID."""
    return account_id + 76561197960265728


def setup_duckdb():
    """Create a DuckDB connection and a table for storing public profiles if it doesn't exist."""
    con = duckdb.connect(database="steam_profiles.duckdb", read_only=False)

    # Drop table if it exists
    con.execute("DROP TABLE IF EXISTS public_profiles")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public_profiles (
        steamid TEXT,
        personaname TEXT,
        profileurl TEXT,
        avatar TEXT,
        avatarmedium TEXT,
        avatarfull TEXT,
        communityvisibilitystate INTEGER,
        personastate INTEGER,
        lastlogoff BIGINT,
        commentpermission INTEGER,
        realname TEXT,
        primaryclanid TEXT,
        timecreated BIGINT,
        loccountrycode TEXT,
        locstatecode TEXT,
        loccityid INTEGER
    );
    """
    con.execute(create_table_sql)
    return con


def append_to_csv(df, csv_file):
    """Append the DataFrame to a CSV file."""
    header = not os.path.exists(csv_file)
    df.to_csv(csv_file, mode="a", header=header, index=False)


def append_without_duplicates(new_df, csv_file, unique_col="steamid"):
    # Check if CSV already exists
    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file)
        # Concatenate the old and new DataFrames
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # Drop duplicates based on the unique column
        combined_df = combined_df.drop_duplicates(subset=unique_col)
    else:
        combined_df = new_df
    # Write the combined DataFrame back to the CSV file
    combined_df.to_csv(csv_file, index=False)


# Function to load the starting account ID from a file, with a default value
def load_start_account_id(default_value=55000000):
    """Loads the last used start_account_id from a file."""
    try:
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return default_value


# Function to save the updated starting account ID to a file
def save_start_account_id(value):
    """Saves the last used start_account_id to a file."""
    with open(STATE_FILE, "w") as f:
        f.write(str(value))

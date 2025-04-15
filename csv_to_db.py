import os

import pandas as pd
import psycopg2
import argparse
from PIL import Image
import io
import time
from sqlalchemy import create_engine, text
from botocore.handlers import disable_signing

from io import StringIO

def create_db_connection():
    """Create and return a database connection."""
    conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)
    return conn

def _populate_product_data(conn, csv_file):
    # Create a string buffer
    # Read the train.csv file into a pandas dataframe, skipping bad lines
    df = pd.read_csv(csv_file, on_bad_lines="skip")
    output = StringIO()
    df_copy = df.copy()

    # Drop rows where any column value is empty
    df_copy = df_copy.dropna()

    # Convert year to integer if it's not already
    df_copy['year'] = df_copy['year'].astype('Int64')

    # Replace NaN with None for proper NULL handling in PostgreSQL
    df_copy = df_copy.replace({pd.NA: None, pd.NaT: None})
    df_copy = df_copy.where(pd.notnull(df_copy), None)
    print("Starting to populate products table")
    # Convert DataFrame to csv format in memory
    df_copy.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)
    # Copy the data to the products table
    with conn.cursor() as cur:
        # Use COPY to insert data
            cur.copy_from(
                file=output,
                table='products',
                null='\\N'
            )

    # Commit and close
    conn.commit()
    print("Finished populating products table")

if __name__ == "__main__":
    conn = create_db_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            img_id TEXT,
            gender VARCHAR(50),
            masterCategory VARCHAR(100),
            subCategory VARCHAR(100),
            articleType VARCHAR(100),
            baseColour VARCHAR(50),
            season TEXT,
            year INTEGER,
            usage TEXT NULL,
            productDisplayName TEXT NULL
        );
    """)

    _populate_product_data(conn, "./datasets/updated_stylesc.csv")
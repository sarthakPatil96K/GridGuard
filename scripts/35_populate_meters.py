import os
import glob
import pandas as pd
import psycopg
from dotenv import load_dotenv


PROJECT_ROOT = r"C:\projects\GridGuard"

CSV_DIR = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "meter_investigation_priority_csv"
)

load_dotenv(
    os.path.join(PROJECT_ROOT, ".env")
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/gridguard"
)


# ------------------------------------------------------------
# FIND STEP 34 CSV
# ------------------------------------------------------------

csv_files = glob.glob(
    os.path.join(CSV_DIR, "part-*.csv")
)

if not csv_files:
    raise FileNotFoundError(
        f"No Step 34 CSV found in:\n{CSV_DIR}"
    )


# ------------------------------------------------------------
# READ METER IDS
# ------------------------------------------------------------

frames = []

for file in csv_files:
    frames.append(
        pd.read_csv(file, usecols=["meter_id"])
    )

df = pd.concat(
    frames,
    ignore_index=True
)

meter_ids = (
    df["meter_id"]
    .dropna()
    .astype(str)
    .str.strip()
    .drop_duplicates()
    .sort_values()
    .tolist()
)

print("=" * 60)
print("GRIDGUARD - POPULATE METERS")
print("=" * 60)

print(f"\nUnique real meters found: {len(meter_ids)}")

print("\nMeter IDs:")
for meter_id in meter_ids:
    print(meter_id)


# ------------------------------------------------------------
# INSERT INTO METERS
# ------------------------------------------------------------

with psycopg.connect(DATABASE_URL) as conn:

    with conn.cursor() as cur:

        for meter_id in meter_ids:

            cur.execute(
                """
                INSERT INTO meters (
                    meter_id,
                    status
                )
                VALUES (%s, 'active')
                ON CONFLICT (meter_id)
                DO NOTHING;
                """,
                (meter_id,)
            )

    conn.commit()


# ------------------------------------------------------------
# VERIFY
# ------------------------------------------------------------

with psycopg.connect(DATABASE_URL) as conn:

    with conn.cursor() as cur:

        cur.execute(
            """
            SELECT COUNT(*)
            FROM meters
            WHERE meter_id LIKE 'MAC%%';
            """
        )

        count = cur.fetchone()[0]

print("\n" + "=" * 60)
print("RESULT")
print("=" * 60)

print(f"\nMAC meters in PostgreSQL: {count}")

if count == len(meter_ids):
    print("\nSUCCESS: All real meters inserted.")
else:
    print("\nWARNING: Meter count does not match.")

print("\nStep 35A completed.")
import os
import requests
import zipfile
import glob
from datetime import datetime, timedelta, UTC
from tqdm import tqdm
import shutil

# ---- CONFIG ----
BASE_EFFECTIVE_DATE = datetime(2026, 2, 19, tzinfo=UTC)  # AIRAC 2602
BASE_AIRAC = (2026, 2)
CYCLE_LENGTH = 28
DATA_ROOT = "data"
# ----------------


def calculate_current_airac(today=None):
    if today is None:
        today = datetime.now(UTC)

    delta_days = (today - BASE_EFFECTIVE_DATE).days
    cycle_offset = delta_days // CYCLE_LENGTH

    year, cycle = BASE_AIRAC
    cycle += cycle_offset

    while cycle > 13:
        cycle -= 13
        year += 1

    while cycle < 1:
        cycle += 13
        year -= 1

    effective_date = BASE_EFFECTIVE_DATE + timedelta(days=cycle_offset * CYCLE_LENGTH)
    return f"{str(year)[-2:]}{cycle:02d}", effective_date


def download_with_progress(url, output_path):
    headers = {"User-Agent": "Mozilla/5.0"}

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))

        with open(output_path, "wb") as f, tqdm(
            desc="Downloading",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:

            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))


def download_and_extract_airac(airac, effective_date):
    airac_folder = os.path.join(DATA_ROOT, airac)
    csv_folder = os.path.join(airac_folder, "csv")
    json_folder = os.path.join(airac_folder, "json")

    if os.path.exists(csv_folder):
        print(f"AIRAC {airac} already exists.")
        return

    os.makedirs(airac_folder, exist_ok=True)

    date_str = effective_date.strftime("%Y-%m-%d")
    url = f"https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_{date_str}.zip"
    zip_path = os.path.join(airac_folder, "nasr.zip")

    print(f"\nDownloading AIRAC {airac}")
    download_with_progress(url, zip_path)

    print("\nExtracting ZIP...")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(airac_folder)

    # nested folder

    csv_zip_pattern = os.path.join(airac_folder, "CSV_DATA", "*_CSV.zip")
    csv_zip_files = glob.glob(csv_zip_pattern)

    if not csv_zip_files:
        raise FileNotFoundError("Could not find nested CSV ZIP inside CSV_DATA folder.")

    nested_csv_zip = csv_zip_files[0]

    print(f"Extracting nested CSV ZIP: {os.path.basename(nested_csv_zip)}")


    os.makedirs(csv_folder, exist_ok=True)
    os.makedirs(json_folder, exist_ok=True)

    with zipfile.ZipFile(nested_csv_zip, 'r') as zip_ref:
        zip_ref.extractall(csv_folder)

    download_preferred_routes(csv_folder)

    cleanup_airac_folder(airac_folder)

    print(f"AIRAC {airac} CSV data ready in {csv_folder}")

def download_preferred_routes(csv_output_folder):
    url = "https://www.fly.faa.gov/rmt/data_file/prefroutes_db.csv"
    output_path = os.path.join(csv_output_folder, "faa.csv")

    print("Downloading FAA preferred routes database...")

    headers = {"User-Agent": "Mozilla/5.0"}

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))

        with open(output_path, "wb") as f, tqdm(
            desc="Downloading faa.csv",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    print("Preferred routes saved as faa.csv")

def cleanup_airac_folder(airac_folder, keep_folders=("csv", "json")):
    for item in os.listdir(airac_folder):
        item_path = os.path.join(airac_folder, item)
        if item not in keep_folders:
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)
    print(f"Cleaned up {airac_folder}, kept only {keep_folders}")

def ensure_current_airac():
    airac, effective_date = calculate_current_airac()
    download_and_extract_airac(airac, effective_date)





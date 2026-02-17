import csv
import json
from star import parse_star
from sid import parse_sid
from airac import ensure_current_airac, calculate_current_airac
import shutil
from utils import csv_to_json
from db import update
import asyncio

airac = calculate_current_airac()[0]
ensure_current_airac()

# fix

# Fields to include in the JSON
fix_fields = [
    "FIX_ID",
    "LAT_DECIMAL",
    "LONG_DECIMAL",
]


fix_csv = f"data/{airac}/csv/FIX_BASE.csv"
fix_json = f"data/{airac}/json/fixes.json"

csv_to_json(fix_csv, fix_json, fix_fields)

# nav

src = f"data/{airac}/csv/NAV_BASE.csv"
dst = f"data/{airac}/csv/NAV_BASE_UTF8.csv"

with open(src, encoding="latin-1") as f:
    text = f.read()

with open(dst, "w", encoding="utf-8", newline="") as f:
    f.write(text)

print("Converted to UTF-8:", dst)

nav_fields = [
    "NAV_ID",
    "LAT_DECIMAL",
    "LONG_DECIMAL",
    "NAME"
]

nav_csv = f"data/{airac}/csv/NAV_BASE_UTF8.csv"
nav_json = f"data/{airac}/json/nav.json"

csv_to_json(nav_csv, nav_json, nav_fields)

# awy

awy_csv = f"data/{airac}/csv/AWY_BASE.csv"
awy_json = f"data/{airac}/json/awy.json"

awy_fields = [
    "AWY_ID",
    "AIRWAY_STRING",
]

csv_to_json(awy_csv, awy_json, awy_fields)

# apt

apt_csv = f"data/{airac}/csv/APT_BASE.csv"
apt_json = f"data/{airac}/json/apt.json"

# Fields to include in the JSON
apt_fields = [
    "ARPT_ID",
    "LAT_DECIMAL",
    "LONG_DECIMAL",
]

csv_to_json(apt_csv, apt_json, apt_fields)


###

parse_sid(f"data/{airac}/csv/DP_BASE.csv", f"data/{airac}/csv/DP_RTE.csv", f"data/{airac}/csv/sid.csv")
parse_star(f"data/{airac}/csv/STAR_BASE.csv", f"data/{airac}/csv/STAR_RTE.csv", f"data/{airac}/csv/star.csv")

sids_csv = f"data/{airac}/csv/sid.csv"
sids_json = f"data/{airac}/json/sid.json"

stars_csv = f"data/{airac}/csv/star.csv"
stars_json = f"data/{airac}/json/star.json"

# Fields to include in the JSON
sid_fields = [
    "sid_name",
    "served_arpt",
    "fixes",
]

star_fields = [
    "star_name",
    "served_arpt",
    "fixes",
]

csv_to_json(sids_csv, sids_json, sid_fields)
csv_to_json(stars_csv, stars_json, star_fields)


# FAA routes

# NOTE - ALWAYS REMOVE BOM ON FILES DOWNLOADED FROM FAA PREFROUTES SITE

faa_csv = f"data/{airac}/csv/faa.csv"
faa_json = f"data/{airac}/json/faa.json"

faa_fields = [
    "Orig",
    "Route String",
    "Dest",
    "Aircraft",
    "Direction"
]

csv_to_json(faa_csv, faa_json, faa_fields)

asyncio.run(update(airac))
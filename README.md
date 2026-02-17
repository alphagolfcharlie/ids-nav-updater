# FAA Navdata Updater

This Python project downloads FAA NASR and preferred routes data, processes the CSVs, converts them to JSON, and loads them into a Prisma database. 
It is designed for updating the NavData of my virtual information display system (vIDS).
---

## Features

- Automatically calculates current AIRAC cycle.
- Downloads FAA NASR ZIP files and extracts CSVs.
- Downloads FAA prefroutes routes CSV (`faa.csv`). 
- Converts CSVs to JSON.
- Loads data into a database using Python Prisma client.
- Handles Airports, Airways, Fixes, Navaids, SIDs, STARs, and FAA routes.


---

## Requirements

- Database running with Prisma ORM 
- 500MB+ space (you may delete the FAA NASR data after the process is complete)

---

## Installation

1. Clone the repository, create a virtual environment and install dependencies: 

```bash
git clone https://github.com/alphagolfcharlie/ids-nav-updater.git
cd ids-nav-updater
python -m venv .venv
.venv\scripts\activate # powershell
source .venv\bin\activate # mac/linux 
pip install -r requirements.txt
```

2. Generate the Prisma Python client 
```bash
prisma generate
```

--- 

## Usage 

Ensure the database URL is set correctly in .env, and then run: 

```bash
python main.py
```

--- 

## Notes

- The FAA prefroutes DB automatically comes with a BOM (byte order mark). The script should remove this, but if there are errors you may need to do so manually. 
- If there are any custom routes aside from those in the FAA DB, these must be added separately. 
- The script traverses table-by-table, deleting all records and inserting new ones. 
- If you run the script while there is no new AIRAC, it will still delete and re-add the data. 
- It is normal for the script to take some time (>30s) to run. 
import json, asyncio
from prisma import Prisma

async def load_entity(db, airac: str, json_file: str, model_name: str, mapping: dict):
    file_path = f"data/{airac}/json/{json_file}"

    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        row = {db_field: item[csv_field] for csv_field, db_field in mapping.items() if csv_field in item}
        sanitized.append(row)

    model = getattr(db, model_name)

    result = await model.create_many(data=sanitized, skip_duplicates=True)
    print(f"{model_name}: inserted {result} records")

async def load_faa_routes(db, airac: str):
    file_path = f"data/{airac}/json/faa.json"

    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        sanitized.append({
            "dep": item.get("Orig"),
            "dest": item.get("Dest"),
            "route": item.get("Route String"),
            "altitude": None,
            "notes": " ".join(filter(None, [item.get("Aircraft"), item.get("Direction")])),
            "source": "faa",
        })

    result = await db.route.create_many(data=sanitized, skip_duplicates=True)
    print(f"FAA routes: inserted {result} records")

async def load_airways(db, airac: str):
    file_path = f"data/{airac}/json/awy.json"

    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        sanitized.append({
            "awy_code": item.get("AWY_ID"),
            "fixes": item.get("AIRWAY_STRING", "").split(),
        })

    result = await db.airway.create_many(data=sanitized, skip_duplicates=True)
    print(f"Airways: inserted {result} records")

async def load_sids(db, airac: str):
    file_path = f"data/{airac}/json/sid.json"

    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        sanitized.append({
            "sid_code": item.get("sid_name"),
            "apts": item.get("served_arpt", "").split(),
            "fixes": item.get("fixes", "").split(),
        })

    result = await db.sid.create_many(data=sanitized, skip_duplicates=True)
    print(f"SIDs: inserted {result} records")

async def load_stars(db, airac: str):
    file_path = f"data/{airac}/json/star.json"

    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        sanitized.append({
            "star_code": item.get("star_name"),
            "apts": item.get("served_arpt", "").split(),
            "fixes": item.get("fixes", "").split(),
        })

    result = await db.star.create_many(data=sanitized, skip_duplicates=True)
    print(f"STARs: inserted {result} records")

async def update(airac: str):
    db = Prisma()
    await db.connect()

    print("Clearing existing nav data...")

    await db.fix.delete_many()
    await db.airway.delete_many()
    await db.route.delete_many()
    await db.sid.delete_many()
    await db.star.delete_many()
    await db.airport.delete_many()

    print("Loading new AIRAC data...")

    await load_entity(db, airac, "apt.json", "airport", {
        "ARPT_ID": "code",
        "LAT_DECIMAL": "lat",
        "LONG_DECIMAL": "lon"
    })

    await load_airways(db, airac)

    await load_faa_routes(db, airac)

    await load_entity(db, airac, "fixes.json", "fix", {
        "FIX_ID": "fix_id",
        "LAT_DECIMAL": "lat",
        "LONG_DECIMAL": "lon"
    })

    await load_entity(db, airac, "nav.json", "fix", {
        "NAV_ID": "fix_id",
        "LAT_DECIMAL": "lat",
        "LONG_DECIMAL": "lon",
        "NAME": "nav_name"
    })

    await load_sids(db, airac)
    await load_stars(db, airac)

    await db.disconnect()

    print("AIRAC update complete.")


import json, asyncio
from prisma import Prisma

async def load_entity(airac: str, json_file: str, model_name: str, mapping: dict):
    db = Prisma()
    await db.connect()

    file_path = f"data/{airac}/json/{json_file}"
    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        row = {db_field: item[csv_field] for csv_field, db_field in mapping.items() if csv_field in item}
        sanitized.append(row)

    # Delete all existing
    model = getattr(db, model_name)
    await model.delete_many()

    # Insert
    result = await model.create_many(data=sanitized, skip_duplicates=True)
    print(f"{model_name}: inserted {result} records")

    await db.disconnect()

async def load_faa_routes(airac: str):
    db = Prisma()
    await db.connect()

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

    await db.route.delete_many()
    result = await db.route.create_many(data=sanitized, skip_duplicates=True)
    print(f"FAA routes:  inserted {result} records")

    await db.disconnect()

async def load_airways(airac: str):
    db = Prisma()
    await db.connect()

    file_path = f"data/{airac}/json/awy.json"
    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    sanitized = []
    for item in data:
        sanitized.append({
            "awy_code": item.get("AWY_ID"),
            "fixes": item.get("AIRWAY_STRING", "").split(),
        })

    await db.airway.delete_many()
    result = await db.airway.create_many(data=sanitized, skip_duplicates=True)
    print(f"Airways: inserted {result} records")

    await db.disconnect()

async def load_sids(airac: str):
    db = Prisma()
    await db.connect()

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

    await db.sid.delete_many()
    result = await db.sid.create_many(data=sanitized, skip_duplicates=True)
    print(f"SIDs: inserted {result} records")

    await db.disconnect()

async def load_stars(airac: str):
    db = Prisma()
    await db.connect()

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

    await db.star.delete_many()
    result = await db.star.create_many(data=sanitized, skip_duplicates=True)
    print(f"STARs: inserted {result} records")

    await db.disconnect()

async def update(airac: str):
    await load_entity(airac, "apt.json", "airport", {
        "ARPT_ID": "code",
        "LAT_DECIMAL": "lat",
        "LONG_DECIMAL": "lon"
    })
    await load_airways(airac)
    await load_faa_routes(airac)
    await load_entity(airac, "fixes.json", "fix", {
        "FIX_ID": "fix_id",
        "LAT_DECIMAL": "lat",
        "LONG_DECIMAL": "lon"
    })
    await load_entity(airac, "nav.json", "fix", {
        "NAV_ID": "fix_id",
        "LAT_DECIMAL": "lat",
        "LONG_DECIMAL": "lon",
        "NAME": "nav_name"
    })

    await load_sids(airac)
    await load_stars(airac)



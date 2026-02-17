import csv, json

def csv_to_json(input_csv, output_json, fields_to_keep):
    # Read CSV and convert to JSON
    data = []
    with open(input_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Keep only the selected fields
            filtered = {field: row[field] for field in fields_to_keep if field in row}
            data.append(filtered)

    # Write JSON output
    with open(output_json, "w", encoding="utf-8") as jsonfile:
        json.dump(data, jsonfile, indent=4)

    print(f"Successfully wrote {len(data)} records to {output_json}")
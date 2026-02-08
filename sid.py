import csv
from collections import defaultdict

def parse_sid(basefile, routefile, outfile):
    sidData = []
    sids = []
    sid_served_airports = {}  # NEW: store served airport per SID

    # --- STEP 1: Load SIDs and store served airport ---
    with open(basefile, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            sid_name = row[-3]
            served_arpt = row[-1]
            if (sid_name, served_arpt) not in sidData:
                sidData.append((sid_name, served_arpt))
                sids.append(sid_name)
                sid_served_airports[sid_name] = served_arpt  # store airport

    # --- STEP 2: Load BODY routes ---
    sid_base_routes = []

    with open(routefile, newline='') as csvfile:
        for row in csv.reader(csvfile):
            if row[4] == "BODY":  # body portion
                sid_base_routes.append([row[3], row[9], int(row[8]), row[-1]])  # SID name, fix name, point_seq, arpt_runway_assoc

    # Group by (SID name, airport/runway group)
    grouped_routes = defaultdict(list)
    for route in sid_base_routes:
        key = (route[0], route[3])
        grouped_routes[key].append(route)

    # Sort and build fixes list per group
    grouped_output = defaultdict(list)
    for (sid_name, airport_group), routes in grouped_routes.items():
        routes.sort(key=lambda r: r[2], reverse=True)
        fix_names = [r[1] for r in routes]
        grouped_output[sid_name].append(fix_names)

    # Merge routes across airport groups (common fixes)
    merged_body_fixes = {}  # {sid_name: [common fixes]}
    for sid_name, all_fix_groups in grouped_output.items():
        common_fixes = set(all_fix_groups[0])
        for fixes in all_fix_groups[1:]:
            common_fixes &= set(fixes)
        ordered_common_fixes = [f for f in all_fix_groups[0] if f in common_fixes]
        merged_body_fixes[sid_name] = ordered_common_fixes

    # --- STEP 3: Load TRANSITION routes ---
    transition_routes = defaultdict(list)

    with open(routefile, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[4] == "TRANSITION":
                transition_name = row[7]  # e.g. KAYLN3.SMUUV
                point = row[9]           # Fix name (POINT)
                point_seq = int(row[8])  # Sequence number
                transition_routes[transition_name].append((point, point_seq))

    # --- STEP 4: Merge transitions with body fixes ---
    final_routes = []

    # Include base SIDs first
    for sid_name, fixes in merged_body_fixes.items():
        route_string = f"{sid_name}:{' '.join(fixes)}"
        final_routes.append(route_string)

    # Transitions
    for transition_name, fixes in transition_routes.items():
        fixes.sort(key=lambda x: x[1], reverse=True)
        transition_fixes = [f[0] for f in fixes]

        # Derive SID name from transition (e.g. KAYLN3.SMUUV => KAYLN3.KAYLN)
        parts = transition_name.split(".")
        if len(parts) == 2:
            sid_prefix = parts[0]
            matched_sid = next((s for s in merged_body_fixes.keys() if s.startswith(sid_prefix)), None)
        else:
            matched_sid = None

        if matched_sid and matched_sid in merged_body_fixes:
            # Merge: prepend body fixes
            transition_fixes = merged_body_fixes[matched_sid] + transition_fixes[1:]

        route_string = f"{transition_name}:{' '.join(transition_fixes)}"
        final_routes.append(route_string)

    # --- STEP 5: Write to CSV with served airport ---

    with open(outfile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sid_name", "served_arpt", "fixes"])

        for route in final_routes:
            if ":" in route:
                sid_name, fix_seq = route.split(":", 1)
                # Find the served airport (use matched SID for transitions)
                parts = sid_name.split(".")
                if len(parts) == 2:
                    sid_prefix = parts[0]
                    matched_sid = next((s for s in merged_body_fixes.keys() if s.startswith(sid_prefix)), None)
                else:
                    matched_sid = sid_name

                served_arpt = sid_served_airports.get(matched_sid, "")
                writer.writerow([sid_name, served_arpt, fix_seq])

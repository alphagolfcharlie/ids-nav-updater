import csv
from collections import defaultdict

def parse_star(basefile, routefile, outfile):
    starData = []
    stars = []
    star_served_airports = {}  # NEW: store served airport per SID

    # --- STEP 1: Load STARs and store served airport ---
    with open(basefile, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            star_name = row[6]
            served_arpt = row[7]
            if (star_name, served_arpt) not in starData:
                starData.append((star_name, served_arpt))
                stars.append(star_name)
                star_served_airports[star_name] = served_arpt  # store airport


    # --- STEP 2: Load BODY routes ---
    star_base_routes = []

    with open(routefile, newline='') as csvfile:
        for row in csv.reader(csvfile):
            if row[3] == "BODY":  # body portion
                star_base_routes.append([row[1], row[8], int(row[7]), row[-1]])  # SID name, fix name, point_seq, arpt_runway_assoc


    # Group by (STAR name, airport/runway group)
    grouped_routes = defaultdict(list)
    for route in star_base_routes:
        key = (route[0], route[3])
        grouped_routes[key].append(route)


    # Sort and build fixes list per group
    grouped_output = defaultdict(list)
    for (star_name, airport_group), routes in grouped_routes.items():
        routes.sort(key=lambda r: r[2], reverse=True)
        fix_names = [r[1] for r in routes]
        grouped_output[star_name].append(fix_names)


    # Merge routes across airport groups (common fixes)
    merged_body_fixes = {}  # {star_name: [common fixes]}
    for star_name, all_fix_groups in grouped_output.items():
        common_fixes = set(all_fix_groups[0])
        for fixes in all_fix_groups[1:]:
            common_fixes &= set(fixes)
        ordered_common_fixes = [f for f in all_fix_groups[0] if f in common_fixes]
        merged_body_fixes[star_name] = ordered_common_fixes



    # --- STEP 3: Load TRANSITION routes ---
    transition_routes = defaultdict(list)

    with open(routefile, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[3] == "TRANSITION":
                transition_name = row[6]  # e.g. KAYLN3.SMUUV
                point = row[8]           # Fix name (POINT)
                point_seq = int(row[7])  # Sequence number
                transition_routes[transition_name].append((point, point_seq))


    # --- STEP 4: Merge transitions with body fixes ---
    final_routes = []

    # Include base stars first
    for star_name, fixes in merged_body_fixes.items():
        route_string = f"{star_name}:{' '.join(fixes)}"
        final_routes.append(route_string)

    print(final_routes)

    # Transitions
    for transition_name, fixes in transition_routes.items():
        fixes.sort(key=lambda x: x[1], reverse=True)
        transition_fixes = [f[0] for f in fixes]

        print(transition_fixes)

        # Derive STAR name from transition (e.g. BOBTA.TPGUN2 => TPGUN.TPGUN2)
        parts = transition_name.split(".")
        if len(parts) == 2:
            star_suffix = parts[1]
            matched_star = next((s for s in merged_body_fixes.keys() if s.endswith(star_suffix)), None)
        else:
            matched_star = None

        if matched_star and matched_star in merged_body_fixes:
            # Merge: prepend body fixes
            transition_fixes = transition_fixes[:-1] + merged_body_fixes[matched_star]

        route_string = f"{transition_name}:{' '.join(transition_fixes)}"
        final_routes.append(route_string)

        #print(final_routes)

    # --- STEP 5: Write to CSV with served airport ---

    with open(outfile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["star_name", "served_arpt", "fixes"])

        for route in final_routes:
            if ":" in route:
                star_name, fix_seq = route.split(":", 1)
                # Find the served airport (use matched SID for transitions)
                parts = star_name.split(".")
                if len(parts) == 2:
                    star_suffix = parts[1]
                    matched_star = next((s for s in merged_body_fixes.keys() if s.endswith(star_suffix)), None)
                else:
                    matched_star = star_name

                served_arpt = star_served_airports.get(matched_star, "")
                writer.writerow([star_name, served_arpt, fix_seq])


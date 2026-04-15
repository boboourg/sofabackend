import os, re, json
from collections import Counter, defaultdict

reports_dir = r"C:\Users\bobur\Desktop\sofascore\reports"
entity_fields = {}  # entity_name -> {field: type}

for fname in os.listdir(reports_dir):
    if not fname.endswith('.md') or fname == 'tample.md':
        continue
    with open(os.path.join(reports_dir, fname), 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find all Entity blocks
    blocks = re.findall(r'## Entity: `?([^`\n]+)`?\s*\n(.*?)(?=\n## |\Z)', content, re.DOTALL)
    for entity_name, block in blocks:
        entity_name = entity_name.strip()
        # Extract fields from table - handle varying column counts
        rows = re.findall(r'\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|', block)
        if rows:
            if entity_name not in entity_fields:
                entity_fields[entity_name] = {}
            for field, ftype in rows:
                if field != 'Field':
                    entity_fields[entity_name][field] = ftype

# Find entities that look like standard reusable components
# Group by suffix pattern
suffix_groups = defaultdict(list)
for name in entity_fields:
    parts = name.split('_')
    # Check known suffixes
    for suffix in ['player', 'team', 'category', 'uniquetournament', 'tournament', 
                   'season', 'manager', 'country', 'sport', 'event',
                   'nametranslation', 'shortnametranslation', 'fieldtranslation',
                   'proposedmarketvalueraw', 'score', 'venue']:
        if name.endswith('_' + suffix) or name == suffix:
            suffix_groups[suffix].append(name)

print("=== Reusable entity groups ===")
for suffix, names in sorted(suffix_groups.items(), key=lambda x: -len(x[1])):
    print(f"\n--- {suffix} ({len(names)} occurrences) ---")
    # Show field union from first 3
    all_fields = set()
    for n in names[:5]:
        all_fields.update(entity_fields[n].keys())
    print(f"  Union of fields: {sorted(all_fields)[:20]}")
    
    # Find common fields across all occurrences
    if len(names) >= 2:
        common = set(entity_fields[names[0]].keys())
        for n in names[1:]:
            common &= set(entity_fields[n].keys())
        print(f"  Common fields ({len(common)}): {sorted(common)[:20]}")

# Now specifically analyze the most important ones
print("\n\n=== PLAYER entity details ===")
player_names = [n for n in entity_fields if n.endswith('_player') or n == 'player']
if player_names:
    # Collect all fields with their types
    field_type_counts = defaultdict(Counter)
    for pn in player_names:
        for f, t in entity_fields[pn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== TEAM entity details ===")
team_names = [n for n in entity_fields if n.endswith('_team') or n == 'team']
if team_names:
    field_type_counts = defaultdict(Counter)
    for tn in team_names:
        for f, t in entity_fields[tn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== UNIQUETOURNAMENT entity details ===")
ut_names = [n for n in entity_fields if n.endswith('_uniquetournament') or n == 'uniquetournament']
if ut_names:
    field_type_counts = defaultdict(Counter)
    for un in ut_names:
        for f, t in entity_fields[un].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== CATEGORY entity details ===")
cat_names = [n for n in entity_fields if n.endswith('_category') or n == 'category']
if cat_names:
    field_type_counts = defaultdict(Counter)
    for cn in cat_names:
        for f, t in entity_fields[cn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== SEASON entity details ===")
sea_names = [n for n in entity_fields if n.endswith('_season') or n == 'season']
if sea_names:
    field_type_counts = defaultdict(Counter)
    for sn in sea_names:
        for f, t in entity_fields[sn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== MANAGER entity details ===")
mgr_names = [n for n in entity_fields if n.endswith('_manager') or n == 'manager']
if mgr_names:
    field_type_counts = defaultdict(Counter)
    for mn in mgr_names:
        for f, t in entity_fields[mn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== EVENT entity details ===")
ev_names = [n for n in entity_fields if n.endswith('_event') or n == 'event']
if ev_names:
    field_type_counts = defaultdict(Counter)
    for en in ev_names:
        for f, t in entity_fields[en].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== SCORE entity details ===")
sc_names = [n for n in entity_fields if n.endswith('_score') or n == 'score']
if sc_names:
    field_type_counts = defaultdict(Counter)
    for sn in sc_names:
        for f, t in entity_fields[sn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

print("\n\n=== VENUE entity details ===")
v_names = [n for n in entity_fields if n.endswith('_venue') or n == 'venue']
if v_names:
    field_type_counts = defaultdict(Counter)
    for vn in v_names:
        for f, t in entity_fields[vn].items():
            field_type_counts[f][t] += 1
    for f, tc in sorted(field_type_counts.items()):
        print(f"  {f}: {dict(tc)}")

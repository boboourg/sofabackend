import re

with open(r"C:\Users\bobur\Desktop\sofascore\reports\tample.md", "r", encoding="utf-8") as f:
    content = f.read()

print("=== Quality checks ===")

# 1. Bad param names with dates
bad = re.findall(r'\{[^}]*\d{4}[^}]*\}', content)
print(f"1. Bad date-like params: {bad}")

# 2. array<> (empty)
empty = content.count('array<>')
print(f"2. array<> (empty): {empty}")

# 3. Endpoints with limit/order/offset in path
stat_in_path = re.findall(r'GET /api/v1/[^\n]*(?:limit|order|offset|accumulation|fields|filters)[^\n]*`', content)
print(f"3. Query-like path segments: {len(stat_in_path)}")
for s in stat_in_path[:3]:
    print(f"   {s}")

# 4. Long nested entity names
long_nested = re.findall(r'_fieldtranslation_|_nametranslation|_proposedmarketvalueraw|_teamcolor', content)
print(f"4. Long nested entity names: {len(long_nested)}")

# 5. Map<string, ...> usage
maps = re.findall(r'Map<string,', content)
print(f"5. Map<string, T> references: {len(maps)}")

# 6. $ref usage
refs = re.findall(r'\$ref:', content)
print(f"6. $ref references: {len(refs)}")

# 7. Total endpoints
endpoints = re.findall(r'#### `GET ', content)
print(f"7. Total endpoints: {len(endpoints)}")

# 8. Duplicate results
dups = re.findall(r'`results`.*\n.*`results`', content)
print(f"8. Duplicate 'results' fields: {len(dups)}")

# 9. tournamentTeamEvents with wrong $ref
wrong = re.findall(r'tournamentTeamEvents.*\$ref: Team', content)
print(f"9. tournamentTeamEvents with wrong Team ref: {len(wrong)}")

# 10. Home/away/total merging
merged = re.findall(r'Варианты', content)
print(f"10. Merged variant endpoints: {len(merged)}")

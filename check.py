import re
with open(r"C:\Users\bobur\Desktop\sofascore\reports\tample.md","r",encoding="utf-8") as f:
    c = f.read()
print("Bad params:", len(re.findall(r'\{\d{4}', c)))
print("array<>:", c.count("array<>"))
print("Query in path:", len(re.findall(r'GET.*/(?:limit|order|offset|accumulation)/', c)))
print("_fieldtranslation_:", c.count("_fieldtranslation_"))
print("Map<string, T>:", c.count("Map<string,"))
print("dollar_ref:", c.count("$ref:"))
print("Endpoints:", len(re.findall(r'#### .GET ', c)))
print("tournamentteamevent_1:", c.count("tournamentteamevent_1"))
print("Duplicate results:", len(re.findall(r'results.*\n.*results', c)))
print("Merged variants:", len(re.findall(r"Варианты", c)))

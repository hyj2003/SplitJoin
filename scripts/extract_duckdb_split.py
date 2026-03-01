import re
import csv
import sys
import pandas as pd
log_file = "duckdb_split.log"

start_pattern = re.compile(r"running\s+(\S+)\s+(\S+)")
time_pattern = re.compile(r"total_time:\s*([\d.]+)")
agg_pattern = re.compile(r"agg time:\s*([0-9]+(?:\.[0-9]+)?)\s*seconds")
end_pattern = re.compile(r"END\s*::\s*(.+)\s*::\s*exit_code=(\d+)\s*-----")
join_marker_pattern = re.compile(r"running\s+(q\d+)\s+(\S+)", re.IGNORECASE)
execution_pattern = re.compile(r"execution:\s*\(([^)]*)\)", re.IGNORECASE)
min_pattern = re.compile(r"^\s*([\d.]+)\s+min", re.IGNORECASE)
oom_pattern = re.compile(r"\[OOM\]", re.IGNORECASE)
tle_pattern = re.compile(r"\[TIMEOUT\]", re.IGNORECASE)

results = {}
current_query = None
in_join_section = False
oom_flag = False
tle_flag = False

def shortname(path: str):
    parts = path.strip().split("/")
    for i, p in enumerate(parts):
        if re.fullmatch(r"q\d+", p):
            return "/".join(parts[i:i + 3]) if i + 2 < len(parts) else "/".join(parts[i:])
    return parts[-1]


with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        m = start_pattern.search(line)
        if m:
            current_query = m.group(1) + "/" + m.group(2)
            results[current_query] = {"join_time_sec": 0.0, "status": "OK", "agg_time": 0.0}
            in_join_section = False
            oom_flag = tle_flag = False
            continue
        m = time_pattern.search(line)
        if current_query and m:
            results[current_query]["join_time_sec"] = float(m.group(1))
            if float(m.group(1)) < 0.01:
                print(current_query)
            continue
        m = agg_pattern.search(line)
        if current_query and m:
            seconds = float(m.group(1))
            results[current_query]["agg_time"] = seconds
            continue
        if oom_pattern.search(line):
            oom_flag = True
        if tle_pattern.search(line):
            tle_flag = True

        if current_query and (oom_flag or tle_flag):
            if oom_flag:
                results[current_query]["status"] = "OOM"
            elif tle_flag:
                results[current_query]["status"] = "TLE"
            current_query = None


if not results:
    print("Cannot find any results.")
    sys.exit(1)

# Writedown CSV file
csv_file = "duckdb_results_split.csv"
with open(csv_file, "w", newline="", encoding="utf-8") as cf:
    writer = csv.writer(cf)
    writer.writerow(["query", "join_time_sec", "agg_time", "status"])
    for q, info in results.items():
        writer.writerow([q, f"{info['join_time_sec']:.6f}", f"{info['agg_time']:.6f}", info["status"]])

df = pd.read_csv(csv_file)

df["dataset"] = df["query"].str.extract(r"(\w+)/(\w+)")[1]
df["qnum"] = df["query"].str.extract(r"(q\d+(?:_[A-Za-z0-9]+)?)")

df["_qnum_base"] = df["qnum"].str.extract(r"q(\d+)").astype(int)
df["_qnum_has_suffix"] = df["qnum"].str.contains(r"_").astype(int)

df = df.sort_values(by=["dataset", "_qnum_has_suffix", "_qnum_base"]).reset_index(drop=True)

df = df.drop(columns=["_qnum_base", "_qnum_has_suffix"])

df.to_csv(csv_file, index=False)
print(f"\nSummary saved to {csv_file}")

def format_value(row):
    if row["status"] in ["OOM", "TLE"]:
        return row["status"]
    return f"{row['join_time_sec']:.3f}"

df["display_val"] = df.apply(format_value, axis=1)

result = (
    df.groupby("dataset")["display_val"]
    .apply(lambda x: " & ".join(x))
    .reset_index()
)

# Print results
for _, row in result.iterrows():
    print(f"{row['dataset']}: {row['display_val']}")
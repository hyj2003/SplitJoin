import re
import csv
import sys
import pandas as pd
log_file = "umbra_baseline.log"
start_pattern = re.compile(r"START\s*::\s*(.+)\s*-----")
end_pattern = re.compile(r"END\s*::\s*(.+)\s*::\s*exit_code=(\d+)\s*-----")
join_marker_pattern = re.compile(r"running\s+(q\d+)\s+(\S+)", re.IGNORECASE)
execution_pattern = re.compile(r"execution:\s*\(([^)]*)\)", re.IGNORECASE)
min_pattern = re.compile(r"^\s*([\d.]+)\s+min", re.IGNORECASE)
oom_pattern = re.compile(r"unable to allocate memory", re.IGNORECASE)
tle_pattern = re.compile(r"time limit|timeout|timed out", re.IGNORECASE)

results = {}
current_query = None
in_join_section = False
oom_flag = False
tle_flag = False

def shortname(path: str):
    parts = path.strip().split("/")
    for i, p in enumerate(parts):
        if re.fullmatch(r"q\d+(?:_[A-Za-z0-9]+)?", p):
            return "/".join(parts[i:i + 3]) if i + 1 < len(parts) else p

    return parts[-1]


with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        m = start_pattern.search(line)
        if m:
            current_query = shortname(m.group(1))
            # print(current_query)
            results[current_query] = {"join_time_sec": 0.0, "status": "OK"}
            in_join_section = False
            oom_flag = tle_flag = False
            continue
        if join_marker_pattern.search(line):
            in_join_section = True
            continue

        if re.search(r"-----\s*(START|END)\b", line):
            in_join_section = False
        if in_join_section and current_query:
            exec_m = execution_pattern.search(line)
            if exec_m:
                inside = exec_m.group(1)
                m2 = min_pattern.search(inside)
                if m2:
                    try:
                        val = float(m2.group(1))
                        results[current_query]["join_time_sec"] += val
                    except ValueError:
                        pass

        if oom_pattern.search(line):
            oom_flag = True
        if tle_pattern.search(line):
            tle_flag = True

        end_m = end_pattern.search(line)
        if end_m and current_query:
            if oom_flag:
                results[current_query]["status"] = "OOM"
            elif tle_flag:
                results[current_query]["status"] = "TLE"
            elif end_m.group(2) != "0":
                results[current_query]["status"] = f"EXIT_{end_m.group(2)}"
            current_query = None
            in_join_section = False

if not results:
    print("Cannot find any results.")
    sys.exit(1)

print(f"{'Query':40s} {'JoinTime(sec)':>15s} {'Status':>8s}")
print("-" * 70)
for q, info in results.items():
    print(f"{q:40s} {info['join_time_sec']:15.3f} {info['status']:>8s}")

# Writedown CSV file
csv_file = "umbra_results_binary.csv"
with open(csv_file, "w", newline="", encoding="utf-8") as cf:
    writer = csv.writer(cf)
    writer.writerow(["query", "join_time_sec", "status"])
    for q, info in results.items():
        writer.writerow([q, f"{info['join_time_sec']:.6f}", info["status"]])

df = pd.read_csv(csv_file)

df["dataset"] = df["query"].str.extract(r"(\w+)\.sql")
df["qnum"] = df["query"].str.extract(r"q(\d+)").astype(float)
df = df.sort_values(by=["dataset", "qnum"]).reset_index(drop=True)
df.to_csv(csv_file, index=False)
print(f"\nSummary saved to {csv_file}")
result = (
    df.groupby("dataset")["join_time_sec"]
    .apply(lambda x: " & ".join(f"{v:.3f}" for v in x))
    .reset_index()
)

# Print results
for _, row in result.iterrows():
    print(f"{row['dataset']}: {row['join_time_sec']}")
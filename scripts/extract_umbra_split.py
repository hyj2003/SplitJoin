import re
import csv
import sys
import pandas as pd
log_file = "umbra_split.log"
start_pattern = re.compile(r"START\s*::\s*(.+)\s*-----")
end_pattern = re.compile(r"END\s*::\s*(.+)\s*::\s*exit_code=(\d+)\s*-----")
join_marker_pattern = re.compile(r"\bjoin time\b[:\s]*", re.IGNORECASE)
preprocess_marker_pattern = re.compile(r"\bpreprocessing\b[:\s]*", re.IGNORECASE)
execution_pattern = re.compile(r"execution:\s*\(([^)]*)\)", re.IGNORECASE)
min_pattern = re.compile(r"^\s*([\d.]+)\s+min", re.IGNORECASE)
oom_pattern = re.compile(r"unable to allocate memory", re.IGNORECASE)
tle_pattern = re.compile(r"time limit|timeout|timed out", re.IGNORECASE)

results = {}
current_query = None
in_join_section = False
in_preprocess_section = False
oom_flag = False
tle_flag = False


def shortname(path: str):
    parts = path.strip().split("/")
    for i, p in enumerate(parts):
        if re.fullmatch(r"q\d+(?:_[A-Za-z0-9]+)?", p):
            return "/".join(parts[i:i + 2]) if i + 1 < len(parts) else p

    return parts[-1]



with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        m = start_pattern.search(line)
        if m:
            current_query = shortname(m.group(1))
            results[current_query] = {"join_time_sec": 0.0, "status": "OK", "preprocess_time": 0.0}
            in_join_section = in_preprocess_section = False
            oom_flag = tle_flag = False
            continue
        if join_marker_pattern.search(line):
            in_join_section = True
            in_preprocess_section = False
            continue
        if preprocess_marker_pattern.search(line):
            in_preprocess_section = True
            in_join_section = False
            continue
        if re.search(r"-----\s*(START|END)\b", line):
            in_join_section = False
            in_preprocess_section = False
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
        if in_preprocess_section and current_query:
            exec_m = execution_pattern.search(line)
            # print(line)
            if exec_m:
                inside = exec_m.group(1)
                m2 = min_pattern.search(inside)
                if m2:
                    try:
                        val = float(m2.group(1))
                        results[current_query]["preprocess_time"] += val
                        # print(val, results[current_query]["preprocess_time"])
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
            in_preprocess_section = False


if not results:
    print("Cannot find any results.")
    sys.exit(1)

print(f"{'Query':40s} {'JoinTime(sec)':>15s} {'Status':>8s}")
print("-" * 70)
for q, info in results.items():
    print(f"{q:40s} {info['join_time_sec']:15.2f} {info['status']:>8s}")

# Writedown CSV file
csv_file = "umbra_results_split.csv"
with open(csv_file, "w", newline="", encoding="utf-8") as cf:
    writer = csv.writer(cf)
    writer.writerow(["query", "total_time_sec", "status", "preprocess_time"])
    for q, info in results.items():
        writer.writerow([q, f"{info['join_time_sec']+info['preprocess_time']:.3f}", info["status"], f"{info['preprocess_time']:.3f}"])
df = pd.read_csv(csv_file)

df["dataset"] = df["query"].str.extract(r"split_(\w+)\.sql")
df["qnum"] = df["query"].str.extract(r"(q\d+(?:_[A-Za-z0-9]+)?)")
# print(df["qnum"])
df = df.sort_values(by=["dataset", "qnum"]).reset_index(drop=True)
df.to_csv(csv_file, index=False)
print(f"\nSummary saved to {csv_file}")
print(df)
result = (
    df.groupby("dataset")["total_time_sec"]
    .apply(lambda x: " & ".join(f"{v:.3f}" for v in x))
    .reset_index()
)

# Print results
for _, row in result.iterrows():
    print(f"{row['dataset']}: {row['total_time_sec']}")

print("")
result = (
    df.groupby("dataset")["preprocess_time"]
    .apply(lambda x: " & ".join(f"{v:.3f}" for v in x))
    .reset_index()
)
for _, row in result.iterrows():
    print(f"{row['dataset']}: {row['preprocess_time']}")
# [Artifact] One Join Order Does Not Fit All: Reducing Intermediate Results with Per-Split Query Plans

## Clone the Repo

```
git clone git@github.com:hyj2003/SplitJoin.git
```

## Getting Started

### Dependency

Install DuckDB for Python:

```
pip install duckdb==v1.3.0
```

Install Umbra:

```
docker pull umbradb/umbra:25.07.1
```

### Prepare Datasets

All datasets are available from the links below:
* [WGPB](https://zenodo.org/records/4035223) (wikidata-wcg-filtered.nt.bz2)
* [Orkut](https://snap.stanford.edu/data/com-Orkut.html)
* [GPlus](https://snap.stanford.edu/data/ego-Gplus.html)
* [USPatent](https://snap.stanford.edu/data/cit-Patents.html)
* [Skitter](https://snap.stanford.edu/data/as-Skitter.html)
* [Topcats](https://snap.stanford.edu/data/wiki-topcats.html)

---

## Run End-to-end Tests

### DuckDB

Create several necessary folders first:
```
mkdir -p duckdb_baseline_profile
mkdir -p duckdb_split_profile/{q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11}
```
To generate test results for DuckDB, please run the following commands (`eta: 1d`):

```
python3 duckdb_baseline.py > duckdb_baseline.log
python3 scripts/extract_duckdb_baseline.py
```

To generate test results for SplitJoin in DuckDB, please run the following commands (`eta: 1d`):

```
bash duckdb_split.sh > duckdb_split.log
python3 scripts/extract_duckdb_split.py
```

For intermediate sizes, please see logs in the `duckdb_baseline_profile` folder and the `duckdb_split_profile` folder.

---

### Umbra

To reproduce the results for SplitJoin in Umbra, first generate SQL queries for splits using:

```
bash gen_split_umbra_sql.sh
```

Create an empty database and load the data:

```
docker run -it -v umbra-db:/var/db -v ./:/mnt umbradb/umbra:25.07.1 umbra-sql -createdb /var/db/umbra.db
docker run -it -v umbra-db:/var/db -v ./:/mnt umbradb/umbra:25.07.1 umbra-sql /var/db/umbra.db /mnt/umbra/load.sql
```

Then run the following commands (`eta: 1d`):

```
bash run_umbra_split.sh
python3 scripts/extract_umbra_split.py
```

To reproduce the results for Umbra baseline settings, change the `UMBRA_MULTIWAY` environment variable to `d` or `e`, or comment out this variable (representing enforced binary join, multi-way join, and the default setting respectively), and run:

```
bash run_umbra_baseline.sh
python3 scripts/extract_umbra_baseline.py
```

with each environment setting (`eta: 1d each setting`).

For intermediate sizes, add `EXPLAIN ANALYZE` at the beginning of each query.

---

## Other Experiments

To test the overall time and maximum intermediate sizes under different thresholds, run:

```
python3 effectiveness_study/eval_thresholds.py > effectiveness_study/eval_thresholds.log
```

To test the overall time of splitting one table at a time, run:

```
bash effectiveness_study/split_table.sh > effectiveness_study/split_table.log
```

To test the overall time of arbitrarily choosing a split set, run:

```
bash effectiveness_study/split_arbitrarily.sh > effectiveness_study/split_arbitrarily.log
```
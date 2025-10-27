#!/usr/bin/env bash
# nohup bash run_umbra_baseline.sh >/dev/null 2>&1 &
set -Euo pipefail

# Env Vars
export UMBRA_MULTIWAY=d 
# 'd' for LOG_FILE="umbra_binary.log"
# 'e' for LOG_FILE="umbra_wcoj.log"
# cmt out for LOG_FILE="umbra_default.log"
export UMBRA_DATABASE_QUERYMEMORYSIZE=220G

# ========= Configurable =========
CPU_SET="0-31"
LOG_FILE="umbra_binary.log"
DB_VOL="umbra-db"
HOST_MNT="./"
IMAGE="umbradb/umbra:25.07.1"
DB_PATH="/var/db/umbra.db"
# =======================

# Turn UMBRA_* vars into -e form（remove prefix UMBRA_）
RAW_ENV_VARS="$(env | grep '^UMBRA_' || true)"
if [[ -n "${RAW_ENV_VARS}" ]]; then
  ENV_VARS="$(printf '%s\n' "${RAW_ENV_VARS}" \
    | awk -F '=' '{print substr($1,7) "=" $2}' \
    | sed 's/^/-e /')"
else
  ENV_VARS=""
fi

# Access SQL query list
SQL_LIST="$(cat <<'SQLS'
/mnt/umbra/q1/baseline/wgpb.sql
/mnt/umbra/q1/baseline/orkut.sql
/mnt/umbra/q1/baseline/gplus.sql
/mnt/umbra/q1/baseline/uspatent.sql
/mnt/umbra/q1/baseline/skitter.sql
/mnt/umbra/q1/baseline/topcats.sql

/mnt/umbra/q2/baseline/wgpb.sql
/mnt/umbra/q2/baseline/orkut.sql
/mnt/umbra/q2/baseline/gplus.sql
/mnt/umbra/q2/baseline/uspatent.sql
/mnt/umbra/q2/baseline/skitter.sql
/mnt/umbra/q2/baseline/topcats.sql

/mnt/umbra/q3/baseline/wgpb.sql
/mnt/umbra/q3/baseline/orkut.sql
/mnt/umbra/q3/baseline/gplus.sql
/mnt/umbra/q3/baseline/uspatent.sql
/mnt/umbra/q3/baseline/skitter.sql
/mnt/umbra/q3/baseline/topcats.sql

/mnt/umbra/q4/baseline/wgpb.sql
/mnt/umbra/q4/baseline/orkut.sql
/mnt/umbra/q4/baseline/gplus.sql
/mnt/umbra/q4/baseline/uspatent.sql
/mnt/umbra/q4/baseline/skitter.sql
/mnt/umbra/q4/baseline/topcats.sql

/mnt/umbra/q5/baseline/wgpb.sql
/mnt/umbra/q5/baseline/orkut.sql
/mnt/umbra/q5/baseline/gplus.sql
/mnt/umbra/q5/baseline/uspatent.sql
/mnt/umbra/q5/baseline/skitter.sql
/mnt/umbra/q5/baseline/topcats.sql

/mnt/umbra/q6/baseline/wgpb.sql
/mnt/umbra/q6/baseline/orkut.sql
/mnt/umbra/q6/baseline/gplus.sql
/mnt/umbra/q6/baseline/uspatent.sql
/mnt/umbra/q6/baseline/skitter.sql
/mnt/umbra/q6/baseline/topcats.sql

/mnt/umbra/q7/baseline/wgpb.sql
/mnt/umbra/q7/baseline/orkut.sql
/mnt/umbra/q7/baseline/gplus.sql
/mnt/umbra/q7/baseline/uspatent.sql
/mnt/umbra/q7/baseline/skitter.sql
/mnt/umbra/q7/baseline/topcats.sql

/mnt/umbra/q8/baseline/wgpb.sql
/mnt/umbra/q8/baseline/orkut.sql
/mnt/umbra/q8/baseline/gplus.sql
/mnt/umbra/q8/baseline/uspatent.sql
/mnt/umbra/q8/baseline/skitter.sql
/mnt/umbra/q8/baseline/topcats.sql

/mnt/umbra/q9/baseline/wgpb.sql
/mnt/umbra/q9/baseline/orkut.sql
/mnt/umbra/q9/baseline/gplus.sql
/mnt/umbra/q9/baseline/uspatent.sql
/mnt/umbra/q9/baseline/skitter.sql
/mnt/umbra/q9/baseline/topcats.sql

/mnt/umbra/q10/baseline/wgpb.sql
/mnt/umbra/q10/baseline/orkut.sql
/mnt/umbra/q10/baseline/gplus.sql
/mnt/umbra/q10/baseline/uspatent.sql
/mnt/umbra/q10/baseline/skitter.sql
/mnt/umbra/q10/baseline/topcats.sql

/mnt/umbra/q11/baseline/wgpb.sql
/mnt/umbra/q11/baseline/orkut.sql
/mnt/umbra/q11/baseline/gplus.sql
/mnt/umbra/q11/baseline/uspatent.sql
/mnt/umbra/q11/baseline/skitter.sql
/mnt/umbra/q11/baseline/topcats.sql
SQLS
)"

# Header info
{
  echo "===================== RUN BEGIN $(date '+%F %T') ====================="
  echo "CPU_SET=${CPU_SET}"
  echo "DB_VOL=${DB_VOL}  HOST_MNT=${HOST_MNT}  IMAGE=${IMAGE}"
  echo "ENV_VARS: ${ENV_VARS:-'(none)'}"
} | tee -a "${LOG_FILE}"

run_one() {
  local sql_path="$1"
  {
    echo
    echo "----- $(date '+%F %T') :: START :: ${sql_path} -----"
  } | tee -a "${LOG_FILE}"

  nohup sudo docker run --rm --cpuset-cpus="${CPU_SET}" ${ENV_VARS} \
    -v "${DB_VOL}:/var/db" \
    -v "${HOST_MNT}:/mnt" \
    "${IMAGE}" \
    umbra-sql "${DB_PATH}" "${sql_path}" \
    2>&1 | tee -a "${LOG_FILE}"

  local rc=$?
  {
    echo "----- $(date '+%F %T') :: END   :: ${sql_path} :: exit_code=${rc} -----"
  } | tee -a "${LOG_FILE}"

  return ${rc}
}

# Main loop
while IFS= read -r line; do
  [[ -z "${line}" ]] && continue
  run_one "${line}"
done <<< "${SQL_LIST}"

echo "===================== RUN END   $(date '+%F %T') =====================" | tee -a "${LOG_FILE}"

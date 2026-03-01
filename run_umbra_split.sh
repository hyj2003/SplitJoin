#!/usr/bin/env bash
# nohup bash run_umbra_split.sh >/dev/null 2>&1 &
set -Euo pipefail

# Env Vars
export UMBRA_MULTIWAY=d
export UMBRA_DATABASE_QUERYMEMORYSIZE=220G

# ========= Configurable =========
CPU_SET="0-31"
LOG_FILE="umbra_split.log"
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
/mnt/umbra/q1/split_wgpb.sql
/mnt/umbra/q1/split_orkut.sql
/mnt/umbra/q1/split_gplus.sql
/mnt/umbra/q1/split_uspatent.sql
/mnt/umbra/q1/split_skitter.sql
/mnt/umbra/q1/split_topcats.sql

/mnt/umbra/q2/split_wgpb.sql
/mnt/umbra/q2/split_orkut.sql
/mnt/umbra/q2/split_gplus.sql
/mnt/umbra/q2/split_uspatent.sql
/mnt/umbra/q2/split_skitter.sql
/mnt/umbra/q2/split_topcats.sql

/mnt/umbra/q3/split_wgpb.sql
/mnt/umbra/q3/split_orkut.sql
/mnt/umbra/q3/split_gplus.sql
/mnt/umbra/q3/split_uspatent.sql
/mnt/umbra/q3/split_skitter.sql
/mnt/umbra/q3/split_topcats.sql

/mnt/umbra/q4/split_wgpb.sql
/mnt/umbra/q4/split_orkut.sql
/mnt/umbra/q4/split_gplus.sql
/mnt/umbra/q4/split_uspatent.sql
/mnt/umbra/q4/split_skitter.sql
/mnt/umbra/q4/split_topcats.sql

/mnt/umbra/q5/split_wgpb.sql
/mnt/umbra/q5/split_orkut.sql
/mnt/umbra/q5/split_gplus.sql
/mnt/umbra/q5/split_uspatent.sql
/mnt/umbra/q5/split_skitter.sql
/mnt/umbra/q5/split_topcats.sql

/mnt/umbra/q6/split_wgpb.sql
/mnt/umbra/q6/split_orkut.sql
/mnt/umbra/q6/split_gplus.sql
/mnt/umbra/q6/split_uspatent.sql
/mnt/umbra/q6/split_skitter.sql
/mnt/umbra/q6/split_topcats.sql

/mnt/umbra/q7/split_wgpb.sql
/mnt/umbra/q7/split_orkut.sql
/mnt/umbra/q7/split_gplus.sql
/mnt/umbra/q7/split_uspatent.sql
/mnt/umbra/q7/split_skitter.sql
/mnt/umbra/q7/split_topcats.sql

/mnt/umbra/q8/split_wgpb.sql
/mnt/umbra/q8/split_orkut.sql
/mnt/umbra/q8/split_gplus.sql
/mnt/umbra/q8/split_uspatent.sql
/mnt/umbra/q8/split_skitter.sql
/mnt/umbra/q8/split_topcats.sql

/mnt/umbra/q9/split_wgpb.sql
/mnt/umbra/q9/split_orkut.sql
/mnt/umbra/q9/split_gplus.sql
/mnt/umbra/q9/split_uspatent.sql
/mnt/umbra/q9/split_skitter.sql
/mnt/umbra/q9/split_topcats.sql

/mnt/umbra/q10/split_wgpb.sql
/mnt/umbra/q10/split_orkut.sql
/mnt/umbra/q10/split_gplus.sql
/mnt/umbra/q10/split_uspatent.sql
/mnt/umbra/q10/split_skitter.sql
/mnt/umbra/q10/split_topcats.sql

/mnt/umbra/q11/split_wgpb.sql
/mnt/umbra/q11/split_orkut.sql
/mnt/umbra/q11/split_gplus.sql
/mnt/umbra/q11/split_uspatent.sql
/mnt/umbra/q11/split_skitter.sql
/mnt/umbra/q11/split_topcats.sql

/mnt/umbra/q12/split_wgpb.sql
/mnt/umbra/q12/split_orkut.sql
/mnt/umbra/q12/split_gplus.sql
/mnt/umbra/q12/split_uspatent.sql
/mnt/umbra/q12/split_skitter.sql
/mnt/umbra/q12/split_topcats.sql

/mnt/umbra/q13/split_wgpb.sql
/mnt/umbra/q13/split_orkut.sql
/mnt/umbra/q13/split_gplus.sql
/mnt/umbra/q13/split_uspatent.sql
/mnt/umbra/q13/split_skitter.sql
/mnt/umbra/q13/split_topcats.sql

/mnt/umbra/q1_f/split_wgpb.sql
/mnt/umbra/q1_f/split_orkut.sql
/mnt/umbra/q1_f/split_gplus.sql
/mnt/umbra/q1_f/split_uspatent.sql
/mnt/umbra/q1_f/split_skitter.sql
/mnt/umbra/q1_f/split_topcats.sql

/mnt/umbra/q2_f/split_wgpb.sql
/mnt/umbra/q2_f/split_orkut.sql
/mnt/umbra/q2_f/split_gplus.sql
/mnt/umbra/q2_f/split_uspatent.sql
/mnt/umbra/q2_f/split_skitter.sql
/mnt/umbra/q2_f/split_topcats.sql

/mnt/umbra/q3_f/split_wgpb.sql
/mnt/umbra/q3_f/split_orkut.sql
/mnt/umbra/q3_f/split_gplus.sql
/mnt/umbra/q3_f/split_uspatent.sql
/mnt/umbra/q3_f/split_skitter.sql
/mnt/umbra/q3_f/split_topcats.sql
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

#!/usr/bin/env bash
# lintap-pull: fetch raw_sensor parquet for a day range, optionally one host / event type(s)
set -euo pipefail

usage() {
  cat <<EOF
Usage: $(basename "$0") -s YYYYMMDD -e YYYYMMDD [-H hostname] [-t type[,type...]] [-d dest] [-n]
  -s  start day (inclusive)
  -e  end day (inclusive)
  -H  hostname (exact match on filename prefix)
  -t  comma-separated event types (default: everything under raw_sensor)
  -d  local destination (default: ./raw_sensor)
  -n  dry run
Env: BASE (default s3://lintap-alpha/lintap/raw_sensor); AWS_PROFILE / AWS_ENDPOINT_URL honored as usual
EOF
  exit 1
}

next_day() {
  if date -u -d "$1" +%Y%m%d >/dev/null 2>&1; then
    date -u -d "$1 + 1 day" +%Y%m%d          # GNU
  else
    date -u -j -v+1d -f %Y%m%d "$1" +%Y%m%d  # BSD/macOS
  fi
}

BASE="${BASE:-s3://lintap-alpha/lintap/raw_sensor}"
DEST=./raw_sensor HOST="" TYPES="" START="" END=""
EXTRA=(--no-progress)

while getopts "s:e:H:t:d:nh" o; do
  case $o in
    s) START=$OPTARG ;; e) END=$OPTARG ;; H) HOST=$OPTARG ;;
    t) TYPES=$OPTARG ;; d) DEST=$OPTARG ;; n) EXTRA+=(--dryrun) ;; *) usage ;;
  esac
done
[[ $START =~ ^[0-9]{8}$ && $END =~ ^[0-9]{8}$ ]] || usage

# Event types: explicit list or discover from the PRE entries
if [[ -n $TYPES ]]; then
  IFS=, read -ra ETS <<< "$TYPES"
else
  ETS=()
  while IFS= read -r et; do ETS+=("$et"); done < <(aws s3 ls "$BASE/" | awk '$1=="PRE"{sub("/$","",$2); print $2}')
fi
(( ${#ETS[@]} )) || { echo "no event types found under $BASE" >&2; exit 1; }

# Host filter: keys are relative to the dayPK prefix, i.e. hourPK=NN/<host>+<type>+<ts>.parquet
[[ -n $HOST ]] && EXTRA+=(--exclude '*' --include "*/${HOST}+*")

d=$START
while (( d <= END )); do
  for et in "${ETS[@]}"; do
    aws s3 sync "$BASE/$et/dayPK=$d/" "$DEST/$et/dayPK=$d/" "${EXTRA[@]}"
  done
  d=$(next_day "$d")
done

#!/bin/bash
#
# Script for comparing Prometheus query result output JSONs for histogram buckets.
# It filters out specific instances and le buckets, and converts math output too.
# Input are two JSON files from HTTP query_range API.
#
# Download JQ: https://stedolan.github.io/jq/
#
# set -x -e
JSON_A=$1
JSON_B=$2
if [ -z "$JSON_A" ]; then
  echo To run: $0 json_output_1 json_output_2
  exit 0
fi

LES=$(cat $JSON_A | jq -r '.data.result[].metric.le' | sort | uniq)
INSTANCES=$(cat $JSON_A | jq -r '.data.result[].metric.instance' | sort | uniq)

# Loop over instances
for INST in $INSTANCES; do
  # In data.result, select the timeseries matching le and instance, then produce one line per value,
  # converting the value to a number for normalization as JSON array
  for le in $LES; do
    # NOTE: in fish shell, use (cat ....  | psub) instead of <(cat... )
    echo Comparing le=$le and inst=$INST...
    diff <(cat $JSON_A | jq -c '.data.result[] | select(.metric.le == "'$le'" and .metric.instance == "'$INST'") | .values[] | [ .[0], (.[1] | tonumber) ]') \
         <(cat $JSON_B | jq -c '.data.result[] | select(.metric.le == "'$le'" and .metric.instance == "'$INST'") | .values[] | [ .[0], (.[1] | tonumber) ]')
  done
done

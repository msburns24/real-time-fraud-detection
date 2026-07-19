#!/usr/bin/env bash
# Streaming evidence for the screencast: partitions, consumer-group health,
# and features being recomputed live as new transactions arrive.
#
# Output is captured into variables before filtering: piping `docker exec`
# straight into `head` closes the pipe early and trips SIGPIPE under pipefail.
set -euo pipefail
K=$(docker compose ps -q kafka)
R=$(docker compose ps -q redis)
KB=/opt/kafka/bin

topic=$(docker exec "$K" $KB/kafka-topics.sh --bootstrap-server localhost:9092 \
          --describe --topic transactions)
group=$(docker exec "$K" $KB/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
          --describe --group feature-processor)

echo "== topic =="
echo "$topic" | sed -n '1p'
echo
echo "== consumer group: every partition assigned (lag = live traffic in flight) =="
echo "$group" | awk 'NF {print $2, $3, $4, $5, $6}' | column -t
echo
echo "== features recomputed live =="
echo "t=0s   $(docker exec "$R" redis-cli get features:CUST0001)"
sleep 6
echo "t=6s   $(docker exec "$R" redis-cli get features:CUST0001)"
echo
echo "keys in store: $(docker exec "$R" redis-cli dbsize)"

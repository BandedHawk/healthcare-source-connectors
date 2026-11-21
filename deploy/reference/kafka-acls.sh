#!/usr/bin/env bash
# Each stage's rights on Kafka, for one pipeline (DEPLOYMENT.md, "Broker rights"; README.md here): read its input in
# its own consumer group, send onward, to its dead-letter topic, and to its own input (reprocess). Nothing else, and
# the admin CLIs nothing at all. The same rights, per stage and pipeline, are tried by connector-broker's
# KafkaSecurityIT, which also checks that a stage is refused another pipeline's topics and group.
#
#   PIPELINE=claims KAFKA_BOOTSTRAP=kafka:9093 KAFKA_ADMIN_CONFIG=admin.properties deploy/reference/kafka-acls.sh
#
# The broker needs the standard authorizer with allow.everyone.if.no.acl.found=false, and each stage's SCRAM user,
# made by the broker team with a password from the secret store, for example
#   kafka-configs.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --command-config "$KAFKA_ADMIN_CONFIG" --alter \
#     --entity-type users --entity-name "splitter-$PIPELINE" --add-config "SCRAM-SHA-512=[password=...]"
# typed or piped from that store, never written in a file or shell history. Topics are made by the broker team too
# (auto.create.topics.enable=false).
set -euo pipefail

: "${PIPELINE:?set PIPELINE}" "${KAFKA_BOOTSTRAP:?set KAFKA_BOOTSTRAP}" "${KAFKA_ADMIN_CONFIG:?set KAFKA_ADMIN_CONFIG}"
ACLS="${KAFKA_HOME:-/opt/kafka}/bin/kafka-acls.sh"

acl() {
  "$ACLS" --bootstrap-server "$KAFKA_BOOTSTRAP" --command-config "$KAFKA_ADMIN_CONFIG" --add "$@"
}

# reads <user> <topic>: Read and Describe on the topic, Read on the user's own consumer group.
reads() {
  acl --allow-principal "User:$1" --operation Read --operation Describe --topic "$2"
  acl --allow-principal "User:$1" --operation Read --group "$1"
}

# sends <user> <topic>...: Write and Describe on each topic.
sends() {
  local user="$1"; shift
  for topic in "$@"; do
    acl --allow-principal "User:$user" --operation Write --operation Describe --topic "$topic"
  done
}

p="$PIPELINE"
sends "landing-$p" "$p.landed"
reads "splitter-$p" "$p.landed"
sends "splitter-$p" "$p.split" "$p.landed"
reads "convert-$p" "$p.split"
sends "convert-$p" "$p.records" "$p.convert.dlq" "$p.split"
reads "transform-$p" "$p.records"
sends "transform-$p" "$p.fhir" "$p.fhir.dlq" "$p.records"

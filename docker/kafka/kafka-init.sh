#!/bin/bash
set -e

BROKER=broker:29092
TOPIC=birth.register.avro
SCHEMA_REGISTRY_URL=http://schema-registry:8081
SCHEMA_FILE=/schemas/birth-schema.avsc

# Wait for broker
echo "Waiting for Kafka broker at $BROKER..."
while ! nc -z broker 29092; do
  sleep 2
done
echo "Kafka broker is up."

# Create topic if not exists
kafka-topics --bootstrap-server $BROKER --create --if-not-exists --topic $TOPIC --partitions 3 --replication-factor 1

# Register schema
# Wait for Schema Registry
echo "Waiting for Schema Registry at $SCHEMA_REGISTRY_URL..."
until curl --silent --fail $SCHEMA_REGISTRY_URL/subjects > /dev/null; do
  sleep 2
done
echo "Schema Registry is up."
echo "Registering Avro schema..."
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\":$(jq -Rs . < $SCHEMA_FILE)}" \
  $SCHEMA_REGISTRY_URL/subjects/$TOPIC-value/versions

echo "Done."

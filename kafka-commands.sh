
kubectl port-forward svc/confluent-cp-kafka-headless 9092:9092

kubectl port-forward svc/confluent-cp-schema-registry 8081:8081

---
kafka-topics --create --topic pos-invoice --bootstrap-server $KAFKA_BOOTSTRAP

kafka-topics --create --topic shipment --bootstrap-server $KAFKA_BOOTSTRAP

kafka-topics --create --topic loyalty --bootstrap-server $KAFKA_BOOTSTRAP

kafka-topics --create --topic hadoop-sink --bootstrap-server $KAFKA_BOOTSTRAP

kafka-topics --create --topic store-sales --bootstrap-server $KAFKA_BOOTSTRAP

---
kafka-get-offsets --bootstrap-server $KAFKA_BOOTSTRAP --topic pos-invoice --partitions 0

kafka-get-offsets --bootstrap-server $KAFKA_BOOTSTRAP --topic shipment --partitions 0

kafka-get-offsets --bootstrap-server $KAFKA_BOOTSTRAP --topic loyalty --partitions 0

kafka-get-offsets --bootstrap-server $KAFKA_BOOTSTRAP --topic hadoop-sink --partitions 0

kafka-get-offsets --bootstrap-server $KAFKA_BOOTSTRAP --topic store-sales --partitions 0

---

kafka-avro-console-consumer --topic pos-invoice --partition 0 --offset 0 --property print.key=true --bootstrap-server $KAFKA_BOOTSTRAP

kafka-avro-console-consumer --topic store-sales --partition 0 --offset 0 --property print.key=true --bootstrap-server $KAFKA_BOOTSTRAP

kafka-avro-console-consumer --topic loyalty --partition 0 --offset 0 --property print.key=true --bootstrap-server $KAFKA_BOOTSTRAP

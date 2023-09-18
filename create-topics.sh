#!/bin/bash

docker exec -it broker bash -c \
'kafka-topics --bootstrap-server localhost:9092 --topic orders-by-user --create &&
kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create --config "cleanup.policy=compact" && # since this is a KTable
kafka-topics --bootstrap-server localhost:9092 --topic discounts --create --config "cleanup.policy=compact" &&
kafka-topics --bootstrap-server localhost:9092 --topic orders --create &&
kafka-topics --bootstrap-server localhost:9092 --topic payments --create &&
kafka-topics --bootstrap-server localhost:9092 --topic paid-orders --create'

#!/bin/bash
HOSTNAME="localhost"
PORT=1883
QOS=0
AMOUNT_ACTIVE=100
AMOUNT_PASSIVE=0
DELAY=100
PAYLOAD_FORMAT="%%latency%%"
DEFER_PUBLISHING="--defer-publishing"
TOPIC="ingestao/telemetria"

./MqttLoadSimulator \
    --hostname $HOSTNAME \
    --port $PORT \
    --qos $QOS \
    --amount-active $AMOUNT_ACTIVE \
    --amount-passive $AMOUNT_PASSIVE \
    --delay $DELAY \
    --payload-format $PAYLOAD_FORMAT \
    $DEFER_PUBLISHING \
    --topic $TOPIC
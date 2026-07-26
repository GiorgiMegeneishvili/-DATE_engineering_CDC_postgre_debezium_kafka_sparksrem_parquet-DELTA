#!/bin/bash

case "$1" in

start)
    echo "Starting CDC stack..."
    docker compose up -d
    ;;

stop)
    echo "Stopping CDC stack..."
    docker compose down
    ;;

clean)
    echo "Removing old containers..."
    docker rm -f kafka debezium-connect kafka-ui 2>/dev/null
    ;;

logs)
    docker compose logs -f
    ;;

status)
    docker ps
    ;;

connector)
    echo "Creating Debezium connector..."
    curl -X POST \
      -H "Content-Type: application/json" \
      --data @postgres-connector.json \
      http://localhost:8083/connectors
    ;;

*)
    echo "Usage:"
    echo "./cdc-manager.sh start"
    echo "./cdc-manager.sh stop"
    echo "./cdc-manager.sh clean"
    echo "./cdc-manager.sh logs"
    echo "./cdc-manager.sh status"
    echo "./cdc-manager.sh connector"
    ;;

esac

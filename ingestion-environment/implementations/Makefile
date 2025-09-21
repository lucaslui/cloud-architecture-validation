compose-up:
	docker compose up -d --build

compose-down:
	docker compose down -v

compose-logs:
	docker compose logs -f --tail=100

compose-logs-app:
	docker compose logs -f --tail=100 collector

create-volumes:
	mkdir -p ./volumes/influxdb_data
	mkdir -p ./volumes/kafka_data
	mkdir -p ./volumes/minio_data

delete-volumes:
	rm -rf ./volumes/influxdb_data
	rm -rf ./volumes/kafka_data
	rm -rf ./volumes/minio_data

create-kafka-port-forwarding:
	kubectl -n ingestion-environment-namespace port-forward svc/kafka 9092:9092

run-kafka-ui:
	docker run --rm --network=host \
	--add-host=kafka:127.0.0.1 \
	-e KAFKA_CLUSTERS_0_NAME=hems \
	-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
	provectuslabs/kafka-ui:v0.7.2
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
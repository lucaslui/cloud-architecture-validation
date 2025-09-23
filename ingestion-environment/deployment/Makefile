create-kafka-port-forwarding:
	kubectl -n ingestion-environment-namespace port-forward svc/kafka 9092:9092

create-influxdb-port-forwarding:
	kubectl -n ingestion-environment-namespace port-forward svc/influxdb 8086:8086

create-minio-port-forwarding:
	kubectl -n ingestion-environment-namespace port-forward svc/minio-console 9001:9001

create-redis-port-forwarding:
	kubectl -n ingestion-environment-namespace port-forward svc/redis 6379:6379
	
create-grafana-port-forwarding:
	kubectl -n cattle-monitoring-system port-forward svc/rancher-monitoring-grafana 3000:80

run-kafka-ui:
	docker run -d \
	--name kafka-ui \
	--network=host \
	--add-host=kafka:127.0.0.1 \
	-e KAFKA_CLUSTERS_0_NAME=hems \
	-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
	provectuslabs/kafka-ui:v0.7.2
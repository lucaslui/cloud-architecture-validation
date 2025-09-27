generate-certs:
	openssl req -x509 -nodes -newkey rsa:2048 \
		-keyout broker.key \
		-out broker.crt \
		-days 365 \
		-config openssl-san.cnf

save-certs-in-k8s:
	kubectl -n ingestion-environment-namespace create secret generic vernemq-tls \
		--from-file=cacerts.pem=./broker.crt \
		--from-file=cert.pem=./broker.crt \
		--from-file=key.pem=./broker.key
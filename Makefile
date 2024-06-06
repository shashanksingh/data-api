# Define the network name
NETWORK_NAME="dbt-net"

setup:
	docker network create dbt-net
	brew install coreutils


start:
	./start_trino.sh
	./bootstrap_superset.sh


start-amazon-linux-2:
	docker-compose -f docker-compose.yml build
	docker-compose -f docker-compose.yml up -d
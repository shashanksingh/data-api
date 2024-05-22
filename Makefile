# Define the network name
NETWORK_NAME="dbt-net"

setup:
	docker network create dbt-net
	brew install coreutils


start:
	./start_trino.sh
	./bootstrap_superset.sh
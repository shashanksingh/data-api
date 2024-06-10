# Retrieve the OS name from /etc/os-release
OS_NAME := $(shell grep -E -w 'NAME' /etc/os-release | cut -d'=' -f2 | tr -d '"')

# Define a target that depends on the OS check
.PHONY: check_os
check_os:
ifeq ($(OS_NAME),Amazon Linux)
	@echo "Running on Amazon Linux"
else
	@echo "Not running on Amazon Linux"
	make start
endif


# Define the network name
NETWORK_NAME="dbt-net"

setup:
	docker network create dbt-net
	brew install coreutils


start:
	./start_trino.sh
	./bootstrap_superset.sh



setup-amazon-linux-2:
	mkdir -p $HOME/code
	cd $HOME/code
	sudo  yum install docker htop
	sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
	sudo chmod +x /usr/local/bin/docker-compose
	sudo yum update -y
	sudo yum install git -y
	git -v
	#ssh-keygen -t ed25519 -C "shashank.personal@gmail.com"
	sudo yum groupinstall "Development Tools"
	git clone https://github.com/shashanksingh/data-api
	export DOCKER_CLIENT_TIMEOUT=300
	export COMPOSE_HTTP_TIMEOUT=300


start-amazon-linux-2:
	sudo docker-compose -f docker-compose.yml build
	sudo docker-compose -f docker-compose.yml up -d


deploy:
	docker stack deploy -c docker-compose.yml paxos

build:
	docker build -t db-node-app:latest ./db-node
	docker build -t master-node-app:latest ./master
	docker build -t log-server-app:latest ./log-server
	docker build -t visualization-app:latest ./visualization

clean:
	docker stack rm paxos

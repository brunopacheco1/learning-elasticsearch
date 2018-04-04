# learning-elasticsearch
Reading and Learning Elastic Search documentation and applying it on Java, Node.js and Postman.

## Running Elastic Search

docker run -p 9200:9200 -p 9300:9300 --rm --name elasticsearch -d -e "discovery.type=single-node" -e "cluster.name=docker-cluster" docker.elastic.co/elasticsearch/elasticsearch:6.2.3
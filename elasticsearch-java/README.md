# elasticsearch-nodejs
Applying in Java what I have learned about Elastic.

## Running the tests

Please, execute the script bellow.

docker run -p 9200:9200 -p 9300:9300 --rm --name elasticsearch -d -e "discovery.type=single-node" -e "cluster.name=docker-cluster" docker.elastic.co/elasticsearch/elasticsearch:6.2.3

mvn test
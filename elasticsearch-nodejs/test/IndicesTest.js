const ElasticSearch = require("elasticsearch");

describe("IndicesTest", () => {

    const client = new ElasticSearch.Client({
        host :  "localhost:9200"
        //log : "trace"
    });

    it("index create", (done) => {
        client.indices.create({
            index : "customer",
            body : {
                "settings" : {
                    "number_of_shards" : 1,
                    "number_of_replicas" : 0
                },
                "mappings" : {
                    "_doc" : {
                        "properties" : {
                            "name" : { "type" : "text" }
                        }
                    }
                }
            }
        }).then(response => {
            done();
        }).catch(error => {
            done(error);
        });
    });

    it("index exists", (done) => {
        client.indices.exists({
            index : "customer"
        }).then(exists => {
            if(exists) {
                done();
            } else {
                done("Failed: Index doesn't exists.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("index delete", (done) => {
        client.indices.delete({
            index : "customer"
        }).then(response => {
            if(response.acknowledged) {
                done();
            } else {
                done("Failed: Index wasn't deleted.");
            }
        }).catch(error => {
            done(error);
        });
    });
});
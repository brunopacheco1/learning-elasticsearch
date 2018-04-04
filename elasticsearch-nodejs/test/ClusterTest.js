const ElasticSearch = require("elasticsearch");

describe("ClusterTest", () => {

    const client = new ElasticSearch.Client({
        host :  "192.168.99.100:9200"
        //log : "trace"
    });

    it("cluster health", (done) => {
        client.cat.health({
            format : "json"
        }).then(response => {
            done();
        }).catch(error => {
            done(error);
        });
    });

    it("create index", (done) => {
        client.indices.create({
            index : "exampledsdsdw",
            body : {
                "settings" : {
                    "number_of_shards" : 1,
                    "number_of_replicas" : 0
                },
                "mappings" : {
                    "profile" : {
                        "properties" : {
                            "location" : { "type" : "geo_point" },
                            "username" : { "type" : "text" },
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
});
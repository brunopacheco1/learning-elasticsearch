const ElasticSearch = require("elasticsearch");

describe("ClusterTest", () => {

    const client = new ElasticSearch.Client({
        host :  "localhost:9200"
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

    it("cluster nodes", (done) => {
        client.cat.nodes({
            format : "json"
        }).then(response => {
            done();
        }).catch(error => {
            done(error);
        });
    });
});
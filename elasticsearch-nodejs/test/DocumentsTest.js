const ElasticSearch = require("elasticsearch");

describe("DocumentsTest", () => {

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

    it("document create", (done) => {
        client.create({
            index : "customer",
            type : "_doc",
            id : "1",
            body : {
                name : "John Doe"
            }
        }).then(response => {
            if(response && response.result == "created") {
                done();
            } else {
                done("Failed: Document wasn't created.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document get", (done) => {
        client.get({
            index : "customer",
            type : "_doc",
            id : "1"
        }).then(response => {
            if(response && response.found) {
                done();
            } else {
                done("Failed: Document wasn't found.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document update", (done) => {
        client.update({
            index : "customer",
            type : "_doc",
            id : "1",
            body : {
                doc : {
                    name : "John Doe Updated"
                }
            }
        }).then(response => {
            if(response && response.result == "updated") {
                done();
            } else {
                done("Failed: Document wasn't updated.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document delete", (done) => {
        client.delete({
            index : "customer",
            type : "_doc",
            id : "1"
        }).then(response => {
            if(response && response.result == "deleted") {
                done();
            } else {
                done("Failed: Document wasn't deleted.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document bulk", (done) => {
        client.bulk({
            body : [
                {index : { _index: "customer", _type: "_doc", _id: "1" }},
                {name : "John Doe" },
                {update : { _index: "customer", _type: "_doc", _id : "1" }},
                {doc : { name : "John Doe becomes Jane Doe" }},
                {delete : { _index: "customer", _type: "_doc", _id : "1" }}
            ]
        }).then(response => {
            if(response && !response.errors) {
                done();
            } else {
                done("Failed: Bulk reques has some problem.");
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
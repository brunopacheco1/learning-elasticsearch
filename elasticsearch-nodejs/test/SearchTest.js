const ElasticSearch = require("elasticsearch");
const axios = require("axios");

describe("SearchTest", () => {

    const client = new ElasticSearch.Client({
        host :  "192.168.99.100:9200"
        //log : "trace"
    });

    it("index create", (done) => {
        client.indices.create({
            index : "bank",
            body : {
                "settings" : {
                    "number_of_shards" : 1,
                    "number_of_replicas" : 0
                }
            }
        }).then(response => {
            done();
        }).catch(error => {
            done(error);
        });
    });

    it("document bulk", (done) => {
        axios.get("https://raw.githubusercontent.com/elastic/elasticsearch/master/docs/src/test/resources/accounts.json")
        .then(json => {
            return client.bulk({
                index : "bank",
                type : "_doc",
                body : json.data.split("\n")
            }).then(response => {
                if(response && !response.errors) {
                    return new Promise((resolve, reject) => {
                        //Bulk is async.
                        setTimeout(() => {
                            resolve();
                        }, 5000);
                    }).then(() => done());
                } else {
                    done("Failed: Bulk request has some problem.");
                }
            });
        }).catch(error => {
            done(error);
        });
    }).timeout(10000);

    it("document first search", (done) => {
        client.search({
            index : "bank",
            q : "*",
            sort : "account_number:asc"
        }).then(response => {
            if(response && response.hits.total == 1000) {
                done();
            } else {
                done("Failed: Search has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document query language", (done) => {
        client.search({
            index : "bank",
            body : {
                query: {
                    bool: {
                        must: [
                            { match: { "age": "40" } }
                        ],
                        must_not : [
                            { match : { "state": "ID" } }
                        ]
                    }
                }
            }
        }).then(response => {
            if(response && response.hits.total == 43) {
                done();
            } else {
                done("Failed: Search using query language has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document filtering", (done) => {
        client.search({
            index : "bank",
            body : {
                query : {
                    bool: {
                        must : { match_all: {} },
                        filter : {
                            range: {
                                balance : {
                                    gte : 20000,
                                    lte : 30000
                                }
                            }
                        }
                    }
                }
            }
        }).then(response => {
            if(response && response.hits.total == 217) {
                done();
            } else {
                done("Failed: Search using filters has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document aggregations", (done) => {
        client.search({
            index : "bank",
            body : {
                "size": 0,
                "aggs": {
                    "group_by_state": {
                        "terms": {
                            "field": "state.keyword"
                        }
                    }
                }
            }
        }).then(response => {
            console.log(response);

            const byState = response.aggregations.group_by_state;

            if(byState && byState.doc_count_error_upper_bound == 20 && byState.sum_other_doc_count == 770) {
                done();
            } else {
                done("Failed: Search using aggregations has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("index delete", (done) => {
        client.indices.delete({
            index : "bank"
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
const ElasticSearch = require("elasticsearch");

describe("GeospatialSearchTest", () => {

    const client = new ElasticSearch.Client({
        host :  "localhost:9200"
        //log : "trace"
    });

    it("index create", (done) => {
        client.indices.create({
            index : "example",
            body : {
                settings : {
                    number_of_shards : 1,
                    number_of_replicas : 0
                },
                mappings : {
                    _doc : {
                        properties : {
                            name : { "type" : "text" },
                            location : { "type" : "geo_shape" }
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
            index : "example",
            type : "_doc",
            id : "1",
            body : {
                name : "Wind & Wetter, Berlin, Germany",
                location : {
                    type : "point",
                    coordinates : [13.400544, 52.530286]
                }
            }
        }).then(response => {
            if(response && response.result == "created") {
                return new Promise((resolve, reject) => {
                    //Geospatial indexing is async.
                    setTimeout(() => {
                        resolve();
                    }, 1000);
                }).then(() => done());
            } else {
                done("Failed: Document wasn't created.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document geo query", (done) => {
        client.search({
            index : "example",
            body : {
                query:{
                    bool: {
                        must: {
                            match_all: {}
                        },
                        filter: {
                            geo_shape: {
                                location: {
                                    shape: {
                                        type: "envelope",
                                        coordinates : [[13.0, 53.0], [14.0, 52.0]]
                                    },
                                    relation: "within"
                                }
                            }
                        }
                    }
                }
            }
        }).then(response => {
            if(response && response.hits.total == 1) {
                done();
            } else {
                done("Failed: Geospatial query has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("index delete", (done) => {
        client.indices.delete({
            index : "example"
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
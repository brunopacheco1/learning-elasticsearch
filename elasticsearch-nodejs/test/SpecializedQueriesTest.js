const ElasticSearch = require("elasticsearch");

describe("SpecializedQueriesTest", () => {

    const client = new ElasticSearch.Client({
        host :  "localhost:9200"
        //log : "trace"
    });

    it("index create", (done) => {
        client.indices.create({
            index : "library",
            body : {
                settings : {
                    number_of_shards : 1,
                    number_of_replicas : 0
                },
                mappings : {
                    book : {
                        properties : {
                            title : { "type" : "text" },
                            description : { "type" : "text" },
                            price : { "type" : "integer" },
                            query : { "type" : "percolator" }
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

    it("put query", (done) => {
        client.create({
            index : "library",
            type : "book",
            id : "thinking-books",
            body : {
                query : {
                    match : {
                        title : "Thinking"
                    }
                }
            }
        }).then(response => {
            if(response && response.result == "created") {
                return new Promise((resolve, reject) => {
                    //Async operation.
                    setTimeout(() => {
                        resolve();
                    }, 1000);
                }).then(() => done());
            } else {
                done("Failed: Query wasn't created.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("document bulk", (done) => {
        client.bulk({
            body : [
                { "index":  { "_index": "library", "_type": "book","_id":1 }},
                {"title":"Magic Of Thinking Big", "description":"Millions of people throughout the world have improved their lives using The Magic of Thinking Big. Dr. David J. Schwartz, long regarded as one of the foremost experts on motivation, will help you sell better, manage better, earn more money, and—most important of all—find greater happiness and peace of mind.", "price" : 20 },
                { "index":  { "_index": "library", "_type": "book","_id":2 }},
                {"title":"The Power of Positive Thinking", "description":"The book describes the power positive thinking has and how a firm belief in something, does actually help in achieving it", "price" : 30 },
                { "index":  { "_index": "library", "_type": "book","_id":3 }},
                {"title":"Think and Grow Rich", "description":"Think And Grow Rich has earned itself the reputation of being considered a textbook for actionable techniques that can help one get better at doing anything, not just by rich and wealthy, but also by people doing wonderful work in their respective fields. ", "price" : 10 },
                { "index":  { "_index": "library", "_type": "book","_id":4 }},
                {"title":"The Magic of thinking Big", "description":"First published in 1959, David J Schwartz's classic teachings are as powerful today as they were then. Practical, empowering and hugely engaging, this book will not only inspire you, it will give you the tools to change your life for the better - starting from now.", "price" : 12 },
                { "index":  { "_index": "library", "_type": "book","_id":5 }},
                {"title":"How to Stop Worrying and Start Living", "description":"The book is written to help readers by changing their habit of worrying. The author Dale Carnegie has shared his personal experiences, wherein he was mostly unsatisfied and worried about lot of life situations.", "price" : 14 },
                { "index":  { "_index": "library", "_type": "book","_id":6 }},
                {"title":"Practicing The Power Of Now", "description":"To make the journey into The Power of Now we will need to leave our analytical mind and its false created self, the ego, behind.", "price" : 15 }
            ]
        }).then(response => {
            if(response && !response.errors) {
                return new Promise((resolve, reject) => {
                    //Async operation.
                    setTimeout(() => {
                        resolve();
                    }, 2000);
                }).then(() => done());
            } else {
                done("Failed: Bulk reques has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    }).timeout(3000);

    it("percolate existing document", (done) => {
        client.search({
            index : "library",
            body : {
                query : {
                    percolate : {
                        field: "query",
                        index : "library",
                        type : "book",
                        id : 1
                    }
                }
            }
        }).then(response => {
            if(response && response.hits.total == 1 && response.hits.hits[0].fields._percolator_document_slot.length == 1) {
                done();
            } else {
                done("Failed: Percolate existing document has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("percolate documents", (done) => {
        client.search({
            index : "library",
            body : {
                "query" : {
                    "percolate" : {
                        "field" : "query",
                        "documents" : [ 
                            {"title":"Magic Of Thinking Big", "description":"Millions of people throughout the world have improved their lives using The Magic of Thinking Big. Dr. David J. Schwartz, long regarded as one of the foremost experts on motivation, will help you sell better, manage better, earn more money, and—most important of all—find greater happiness and peace of mind." }
                        ]
                    }
                }
            }
        }).then(response => {
            if(response && response.hits.total == 1 && response.hits.hits[0].fields._percolator_document_slot.length == 1) {
                done();
            } else {
                done("Failed: Percolate documents has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("more_like_this query", (done) => {
        client.search({
            body : {
                "query": {
                    "more_like_this" : {
                        "fields" : ["description"],
                        "like" : ["Think Big","Positive Thinking"],
                        "min_term_freq" : 1,
                        "min_doc_freq":1
                    }
                }
             }
        }).then(response => {
            if(response && response.hits.total == 3) {
                done();
            } else {
                done("Failed: more_like_this query has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("script query", (done) => {
        client.search({
            body : {
                "query": {
                    "bool" : {
                        "must" : {
                            "script" : {
                                "script" : {
                                    "source": "doc['price'].value >= 20",
                                    "lang": "painless"
                                 }
                            }
                        }
                    }
                }
             }
        }).then(response => {
            if(response && response.hits.total == 2) {
                done();
            } else {
                done("Failed: script query has some problem.");
            }
        }).catch(error => {
            done(error);
        });
    });

    it("index delete", (done) => {
        client.indices.delete({
            index : "library"
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
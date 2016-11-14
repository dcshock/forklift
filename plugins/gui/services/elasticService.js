var elasticsearch = require('elasticsearch');
var logger = require('../utils/logger');

var client = new elasticsearch.Client({
    host: (process.env.FK_ES_HOST || 'localhost') + ":" + (process.env.FK_ES_PORT || 9200)
});

var poll = function(service, callback) {
    var logs = [];
    var index = 'forklift-'+service+'*';

    client.search({
        index: index,
        size: 100,
        body: {
            query: {
                query_string: {
                    query: "Error",
                    fields: ["step"]
                }
            },
            "sort": [{
                "time": {"order": "desc"}
            }]
        }
    }).then(function (resp) {
        resp.hits.hits.forEach(function(hit) {
            logs.push(hit);
        });
        callback(logs);
    }, function(err) {
        logger.error(err.message);
        callback(null, err.message);
    });
};

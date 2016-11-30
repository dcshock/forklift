var elasticsearch = require('elasticsearch');
var Stomp = require('stomp-client');
var logger = require('../utils/logger');

var client = new elasticsearch.Client({
    host: (process.env.FK_ES_HOST || 'localhost') + ":" + (process.env.FK_ES_PORT || 9200)
});

var stompClient;
var service = {};

stompConnect();

function stompConnect() {
    logger.info("Connecting stomp client...");
    stompClient = new Stomp(process.env.FK_STOMP_HOST || 'localhost', process.env.FK_STOMP_PORT || 61613, null, null, null, null, {retries: 5, delay: 10000});
    stompClient.connect(function() {
        logger.info("Stomp client connected!");
    });
    stompClient.on('error', function(err) {
        logger.error('STOMP: ' + err.message);
    })
}
service.ping = function(done) {
    client.ping({
        // ping usually has a 3000ms timeout
        requestTimeout: 3000,
        // undocumented params are appended to the query string
        hello: "elasticsearch!"
    }, function (error) {
        if (error) {
            done(false);
        } else {
            done(true);
        }
    });
};

service.get = function(id, done) {
    var index = 'forklift-replay*';
    client.search({
        index: index,
        size: 1,
        body: {
            query: {
                query_string: {
                    query: id,
                    fields: ["_id"]
                }
            }
        }
    }).then(function (resp) {
        done(resp.hits.hits[0]);
    }, function(err) {
        done(null);
    });
};
service.poll = function(service, queue, done) {
    var index = 'forklift-'+service+'*';

    var query;
    if (queue == null) {
        query = {
            query_string: {
                query: "Error",
                fields: ["step"]
            }
        };
    } else {
        query = {
            bool: {
                must: [
                    {match: {"step": "Error"}},
                    {match: {"queue": queue}}
                ]
            }
        }
    }
    client.search({
        index: index,
        size: 50,
        body: {
            query: query,
            "sort": [{
                "time": {"order": "desc"}
            }]
        }
    }).then(function (resp) {
        done(resp.hits.hits);
    }, function(err) {
        logger.error(err.message);
        done(null, err.message);
    });
};

service.update = function(index, updateId, step, done) {
    client.update({
        index: index,
        id: updateId,
        type: 'log',
        body:  {
            doc: {
                step: step
            }
        }
    }, function (err) {
        if (err) {
            logger.error(err);
        }
        done();
    });
};

service.retry = function(correlationId, text, queue, done) {
    var msg = {
        jmsHeaders : { 'correlation-id' : correlationId },
        body : text,
        queue : queue
    };

    logger.info('Sending: ' + msg.jmsHeaders['correlation-id']);
    // messages to the stomp connector should persist through restarts
    msg.jmsHeaders['persistent'] = 'true';
    // special tag to allow non binary msgs
    msg.jmsHeaders['suppress-content-length'] = 'true';
    stompClient.publish(msg.queue, msg.body, msg.jmsHeaders);
    done();
};

service.stats = function(done) {
    getStats('forklift-retry*', function(retryStats) {
        getStats('forklift-replay*', function(replayStats) {
            done({
                replay: replayStats,
                retry: retryStats
            })
        })
    })
};

var getStats = function(index, done) {
    client.search({
        index: index,
        size: 10000,
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
        var size = resp.hits.hits.length;
        var queues = [];
        var queueTotals = [];
        if (size == 0)
            return done({
                totalLogs: 0,
                queues: queues,
                queueTotals: queueTotals
            });
        resp.hits.hits.forEach(function(hit, i) {
            hit = hit._source;
            if (queues.indexOf(hit.queue) > -1) {
                var index = queues.indexOf(hit.queue);
                queueTotals[index] = queueTotals[index] + 1;
            } else  {
                queues.push(hit.queue);
                queueTotals.push(1);
            }
            if (i == (size - 1)) {
                return done({
                    totalLogs: size,
                    queues: queues,
                    queueTotals: queueTotals
                });
            }
        });
    }, function(err) {
        logger.error(err.message);
        done(null);
    });
};
module.exports = service;

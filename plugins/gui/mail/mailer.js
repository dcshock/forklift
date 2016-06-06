var querystring = require('querystring');
var http = require('http');
var fs = require('fs');
var elasticsearch = require('elasticsearch');
var logger = require('../utils/logger');
var request = require('request');
var stompit = require('stompit');

var m = {}

var ip = process.env.CORE_IP || "localhost";
var to = process.env.FG_MAILING_LIST;
var from = process.env.FG_MAILING_FROM;

var activemqHost = null;
var activemqPort = null;

var esClient = new elasticsearch.Client({
    host: (process.env.FK_ES_HOST || 'localhost') + ":" + (process.env.FK_ES_PORT || 9200)
});

var headers = {
    'Content-Type': 'application/json',
    'persistent': 'true',
    'destination': 'email'
}

m.processReplayStatusEmail = function () {
    var index = 'forklift-replay*';
    esClient.search({
        index: index,
        size: 5000,
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
        var message = "<h2>Forklift Replay - Daily Summary</h2>" +
            "<br>" +
            "There are currently <b>" + resp.hits.hits.length + "</b> items in the forklift replay queue that need attention!" +
            "<br>";
        var queueMap = {};

        resp.hits.hits.forEach(function(hit) {
            var source = hit._source;
            if (queueMap[source.queue]) {
                // increase a currently added queue items size
                var size = parseInt(queueMap[source.queue]);
                size++;
                queueMap[source.queue] = size;
            } else {
                // add the queue item to the map
                queueMap[source.queue] = 1;
            }
        });
        for (var q in queueMap) {
            message += "<b>"+q+"</b>: "+queueMap[q] + "<br>";
        }
        logger.info("sending forklift replay status email now");
        sendReplayStatusEmail(message);
    }, function(err) {
        logger.error(err.message);
    });
}

var sendReplayStatusEmail = function (message) {
    request('http://' + ip + ':8500/v1/catalog/semairvice/activemq-broker', function (error, response, body) {
        var json = JSON.parse(body);
        if (!error) {
            activemqHost = json[0].ServiceAddress;
            activemqPort = json[0].ServicePort;

            var connectOptions = {
                host: activemqHost || 'localhost',
                port: 61613,
                connectHeaders: {
                    login: null,
                    passcode: null
                }
            }

            var msg = {
                to: to,
                from: from,
                subject: "Forklift - Daily Summary",
                body: message,
            }

            stompit.connect(connectOptions, function (error, client) {
                if (error) {
                    logger.error('connect error: ' + error.message + ' while trying to connect to activemq host: ' + connectOptions.host + ' activemq port: ' + connectOptions.port);
                    return
                }
                var frame = client.send(headers);
                frame.end(JSON.stringify(msg));

                client.disconnect(function (error) {
                    if (error) {
                        logger.error("Error while disconnecting: " + error.message);
                        return
                    }
                    logger.info("Password change reminder sent!");
                });
            });
        } else {
            logger.error("error while trying to request activemq-broker from consul: "+error.message);
        }
    });
}

module.exports = m;

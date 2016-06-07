var querystring = require('querystring');
var http = require('http');
var fs = require('fs');
var elasticsearch = require('elasticsearch');
var logger = require('../utils/logger');
var request = require('request');
var stompit = require('stompit');
var nodemailer = require('nodemailer');

var m = {}

var smtp_host = process.env.FG_SMTP_HOST;
var smtp_port = process.env.FG_SMTP_PORT;
var smtp_user = process.env.FG_SMTP_USER;
var smtp_pass = process.env.FG_SMTP_PASS;

var to = process.env.FG_MAILING_LIST;
var from = process.env.FG_MAILING_FROM;

var esClient = new elasticsearch.Client({
    host: (process.env.FK_ES_HOST || 'localhost') + ":" + (process.env.FK_ES_PORT || 9200)
});

var smtp_config = {
    host: smtp_host || 'localhost',
    port: smtp_port || 465,
    auth: {
        user: smtp_user,
        pass: smtp_pass
    }
}

var transporter = nodemailer.createTransport(smtp_config);

m.processReplayStatusEmail = function () {
    if (process.env.FG_MAILING_LIST && process.env.FG_MAILING_FROM) {
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
            var message = "<h2>+-- Todays Summary --+</h2>" +
                "<br>" +
                "There are currently <b>" + resp.hits.hits.length + "</b> items in the forklift replay queue that need attention!" +
                "<br>";
            var queueMap = {};

            resp.hits.hits.forEach(function (hit) {
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
                message += "<b>" + q + "</b>: " + queueMap[q] + "<br>";
            }
            logger.info("sending forklift replay status email now");
            sendReplayStatusEmail(message);
        }, function (err) {
            logger.error(err.message);
        });
    } else {
        logger.error("Unable to send out replay status emails due to undefined mailing parameters");
    }
}

var sendReplayStatusEmail = function (message) {
    var mailOptions = {
        to: to,
        from: from,
        subject: "Forklift Replay - Daily Summary",
        html: message,
    }
    transporter.sendMail(mailOptions, function(error, info){
       if (error) {
           return logger.error(error);
       }
       logger.info("Daily Summary Sent! " + info.response);
    });
}

module.exports = m;

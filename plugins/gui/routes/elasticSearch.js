var elasticsearch = require('elasticsearch');
var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;
var express = require('express');
var logger = require('../utils/logger');
var passport = require('passport');
var router = express.Router();

var client = new elasticsearch.Client({
    host: (process.env.FK_ES_HOST || 'localhost') + ":" + (process.env.FK_ES_PORT || 9200)
});

router.post('/poll/', ensureAuthenticated, function (req, res) {
    var service = req.body.service;
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
        var json = {
            log: true,
            response: logs
        }
        res.end(JSON.stringify(json));
    }, function(err) {
        logger.error(err.message);
        var json = {
            log: false,
            response: err
        }
        res.end(JSON.stringify(json));

    });
});

router.post('/fixed/', ensureAuthenticated, function(req, res) {
    var updateId = req.body.id;
    var index = req.body.index;

    client.update({
        index: index,
        id: updateId,
        type: 'log',
        body:  {
            doc: {
                step: 'Fixed'
            }
        }
    }, function (err) {
        if (err) {
            logger.error(err);
        }
    });
    res.end();
});

module.exports = router;

var express = require('express');
var passport = require('passport');
var elasticsearch = require('elasticsearch');
var logger = require('../utils/logger');
var config = require('../config/config');
var router = express.Router();

var client = new elasticsearch.Client({
    host: process.env.NOISE_ES_HOST
});

router.post('/poll/', ensureAuthenticated, function (req, res) {
    var service = req.body.service;
    var logs = [];
    var index = 'forklift-'+service+'*';

    client.search({
        index: index,
        size: 5000,
        body: {
            query: {
                query_string: {
                    query: "Error",
                    fields: ["step"]
                }
            }
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
    var docDate = req.body.date;
    var index = 'forklift-replay-'+docDate;

    client.update({
        index: index,
        id: updateId,
        type: 'log',
        body:  {
            doc: {
                step: 'Fixed'
            }
        }
    }, function (err, resp) {
        if (err) {
            console.log(err);
        }
    });
});

function ensureAuthenticated(req, res, next) {
    if (req.isAuthenticated()) {
        return next();
    }
    logger.info("Unauthorized");
    res.status(401);
    res.render('401');
    return res.statusCode;
}

module.exports = router;




var express = require('express');
var elasticService = require('../services/elasticService.js');
var logger = require('../utils/logger');

module.exports.showLinkedLog = function(req, res) {
    elasticService.get(req.query.id, function(log) {
        if (log == null) {
            req.flash("error", "INVALID LOG ID");
            res.redirect('/')
        }
        res.render('linked-log', {currentUrl: 'replays', log: log})
    });
};
module.exports.updateAsFixed = function(req, res) {
    var updateId = req.body.updateId;
    var index = req.body.index;
    elasticService.update(index, updateId, 'Fixed', function() {
        res.end();
    });
};
module.exports.updateAllAsFixed = function(req, res) {
    var queue = req.body.queue;
    elasticService.poll('replay', queue, function(logs, err) {
        if (logs === 'undefined' || logs == null) {
            req.flash('error', err);
        }
        for (var i = 0; i < logs.length; i++) {
            elasticService.update(logs[i]._index, logs[i]._id, 'Fixed', function() {
            })
        }
        res.send("done");
    });

};

module.exports.retry = function(req, res) {
    var correlationId = req.body.correlationId;
    var text = req.body.text;
    var queue = req.body.queue;
    elasticService.retry(correlationId, text, queue, function() {
        res.end();
    })
};

module.exports.retryAll = function(req, res) {
    var queue = req.body.queue;
    elasticService.poll('replay', queue, function(logs, err) {
        if (logs === 'undefined' || logs == null) {
            req.flash('error', err);
        }
        for (var i = 0; i < logs.length; i++) {
            elasticService.retry(logs[i]._id, logs[i]._source.text, queue, function() {
            })
        }
        res.send("done");
    });
};

module.exports.showRetries = function(req, res) {
    elasticService.poll('retry', null, function(logs, err) {
        if (logs === 'undefined' || logs == null) {
            req.flash('error', err);
        }
        var hits = [];
        for (var i = 0; i < logs.length; i++) {
            hits.push(logs[i]);
        }
        res.render('log-display', {currentUrl: 'retries', hits: hits})
    });
};
module.exports.showReplays = function(req, res) {
    elasticService.poll('replay', null, function(logs, err) {
        if (logs === 'undefined' || logs == null) {
            req.flash('error', err);
        }
        var hits = [];
        for (var i = 0; i < logs.length; i++) {
            hits.push(logs[i]);
        }
        res.render('log-display', {currentUrl: 'replays', hits: hits})
    });
};
module.exports.showFilteredResults = function(req, res) {
    var service = req.query.service;
    var queue = req.query.queue;

    var tempService = service == 'retries' ? 'retry' : 'replay';
    elasticService.poll(tempService, queue, function(logs, err) {
        if (logs === 'undefined' || logs == null) {
            req.flash('error', err);
        }
        var hits = [];
        for (var i = 0; i < logs.length; i++) {
            hits.push(logs[i]);
        }
        res.render('log-display', {currentUrl: service, hits: hits});
    });
}

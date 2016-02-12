var express = require('express');
var Stomp = require('stomp-client');
var fs = require('fs');
var passport = require('passport');
var logger = require('../utils/logger');
var router = express.Router();


router.post('/retry/', ensureAuthenticated, function (req, res) {
    var correlationId = req.body.correlationId;
    var text = req.body.text;
    var queue = req.body.queue;


    var headers = { 'correlation-id' : correlationId };
    var msg = {
        jmsHeaders : headers,
        body : text,
        queue : queue
    };

    var client = new Stomp('localhost', 61613, null, null);
    client.on('error', function(e) {
        console.log(e);
    });

    client.connect(function() {
        console.log("Connected");

        console.log('Sending: ' + msg.jmsHeaders['correlation-id']);

        // messages to the stomp connector should persist through restarts
        msg.jmsHeaders['persistent'] = 'true';

        // special tag to allow non binary msgs
        msg.jmsHeaders['suppress-content-length'] = 'true';

        client.publish(msg.queue, msg.body, msg.jmsHeaders);
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

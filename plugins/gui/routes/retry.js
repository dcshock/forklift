var ensureAuthenticated = require("../utils/auth").ensureAuthenticated;
var express = require('express');
var fs = require('fs');
var logger = require('../utils/logger');
var passport = require('passport');
var router = express.Router();
var Stomp = require('stomp-client');

var client = new Stomp('localhost', 61613, null, null);
client.on('error', function(e) {
    console.log(e);
});

router.post('/retry/', ensureAuthenticated, function (req, res) {
    var correlationId = req.body.correlationId;
    var text = req.body.text;
    var queue = req.body.queue;
    var msg = {
        // jmsHeaders : { 'correlation-id' : correlationId,
        //                'forklift-retry-count': 0,
        //                'forklift-retry-max-retries': 0 },
        jmsHeaders : { 'correlation-id' : correlationId },
        body : text,
        queue : queue
    };

    client.connect(function() {
        logger.info('Sending: ' + msg.jmsHeaders['correlation-id']);

        // messages to the stomp connector should persist through restarts
        msg.jmsHeaders['persistent'] = 'true';

        // special tag to allow non binary msgs
        msg.jmsHeaders['suppress-content-length'] = 'true';

        client.publish(msg.queue, msg.body, msg.jmsHeaders);

        res.end();
    });
});

module.exports = router;
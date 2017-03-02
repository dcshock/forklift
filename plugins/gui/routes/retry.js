var ensureAuthenticated = require("../utils/auth").ensureAuthenticated;
var express = require('express');
var fs = require('fs');
var logger = require('../utils/logger');
var passport = require('passport');
var router = express.Router();
var Stomp = require('stomp-client');

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

    var client = new Stomp(process.env.FK_STOMP_HOST || 'localhost', process.env.FK_STOMP_PORT || 61613, null, null);
    client.on('error', function(e) {
        logger.error(e);
    });

    client.connect(function() {
        logger.info('Sending: ' + msg.jmsHeaders['correlation-id']);

        // messages to the stomp connector should persist through restarts
        msg.jmsHeaders['persistent'] = 'true';

        // special tag to allow non binary msgs
        msg.jmsHeaders['suppress-content-length'] = 'true';

        client.publish(msg.queue, msg.body, msg.jmsHeaders);
        client.disconnect();
        res.end();
    });
});

module.exports = router;

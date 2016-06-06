var CronJob = require('cron').CronJob;
var mailer = require('../mail/mailer');
var logger = require('../utils/logger');

var time = process.env.FG_MAILING_TIME;
var hour = time.split(':')[0];
var minute = time.split(':')[1];

if (hour < 10) {
    var parsed = parseInt(hour);
    hour = "0" + parsed;
}
var job = new CronJob({
    cronTime: '00 '+minute+' '+hour+' * * 0-6',
    onTick: function() {
        logger.info("running forklift daily summary emailer");
        mailer.processReplayStatusEmail();
    },
    start: false,
    timeZone: 'America/Denver'
});
job.start();



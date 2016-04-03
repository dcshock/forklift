var winston = require('winston');
var morgan = require('morgan');

var logger = new winston.Logger({
  transports: [
    new winston.transports.Console({
      level: 'info',
      handleExceptions: true,
      json: true,
      stringify: true,
      timestamp: true
    })
  ],
  exitOnError: false
});

logger.emitErrs = true;

logger.infoStream = {

};
logger.errorStream = {
  write: (message, encoding) => logger.error(message)
}

logger.errorMorgan = morgan('combined', {
    skip: function (req, res) {
        return res.statusCode < 500
    },
    stream: logger.errorStream
});

module.exports = logger;
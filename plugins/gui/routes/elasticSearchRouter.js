var elasticController = require('../controllers/elasticSearchController.js');
var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;

module.exports = function(express) {
    var router = express.Router();
    router.post('/poll/', ensureAuthenticated, elasticController.poll);
    router.post('/fixed/', ensureAuthenticated, elasticController.updateAsFixed);
    router.post('/retry/', ensureAuthenticated, elasticController.retry);
};
